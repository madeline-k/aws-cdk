import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as kms from '@aws-cdk/aws-kms';
import * as redshift from '@aws-cdk/aws-redshift';
import * as s3 from '@aws-cdk/aws-s3';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import * as cdk from '@aws-cdk/core';
import { Construct } from 'constructs';
import { FirehoseRedshiftTable } from './table';
import { RedshiftColumn } from './types';
import { FirehoseRedshiftUser } from './user';

/**
 * The Redshift user Firehose will assume to deliver data to Redshift
 */
export interface RedshiftUser {
  /**
   * Username for user that has permission to insert records into a Redshift table.
   */
  readonly username: string;

  /**
   * Password for user that has permission to insert records into a Redshift table.
   *
   * Do not put passwords in your CDK code directly.
   *
   * @default - a Secrets Manager generated password.
   */
  readonly password?: cdk.SecretValue;

  /**
   * KMS key to encrypt the generated secret.
   *
   * @default - default master key.
   */
  readonly encryptionKey?: kms.IKey;
}

/**
 * Properties for configuring a Redshift delivery stream destination.
 */
export interface RedshiftDestinationProps extends firehose.DestinationProps {
  /**
   * The Redshift cluster to deliver data to.
   */
  readonly cluster: redshift.ICluster;

  /**
   * The cluster user that has INSERT permissions to the desired output table.
   */
  readonly user: RedshiftUser;

  /**
   * The database containing the desired output table.
   */
  readonly database: string;

  /**
   * The table that data should be inserted into.
   */
  readonly tableName: string;

  /**
   * The table columns that the source fields will be loaded into.
   */
  readonly tableColumns: RedshiftColumn[];

  /**
   * The secret that holds Redshift cluster credentials for a user with administrator privileges.
   *
   * Used to create the user that Firehose assumes and the table that data is inserted into.
   *
   * @default - the master secret is taken from the cluster.
   */
  readonly masterSecret?: secretsmanager.ISecret;

  /**
   * Parameters given to the COPY command that is used to move data from S3 to Redshift.
   *
   * @see https://docs.aws.amazon.com/firehose/latest/APIReference/API_CopyCommand.html
   *
   * @default '' - no extra parameters are provided to the Redshift COPY command
   */
  readonly copyOptions?: string;

  /**
   * The length of time during which Firehose retries delivery after a failure.
   *
   * @default Duration.hours(1)
   */
  readonly retryTimeout?: cdk.Duration;

  /**
   * The intermediate bucket where Firehose will stage your data before COPYing it to the Redshift cluster.
   *
   * @default - a bucket will be created for you.
   */
  readonly intermediateBucket?: s3.IBucket;

  /**
   * The role that is attached to the Redshift cluster and will have permissions to access the intermediate bucket.
   *
   * If a role is provided, it must be already attached to the cluster, to avoid the 10 role per cluster limit.
   *
   * @default - a role will be created for you.
   */
  readonly bucketAccessRole?: iam.IRole;

  /**
   * The size of the buffer that Firehose uses for incoming data before delivering it to the intermediate bucket.
   *
   * @default Duration.seconds(60)
   */
  readonly bufferingInterval?: cdk.Duration;

  /**
   * The length of time that Firehose buffers incoming data before delivering it to the intermediate bucket.
   *
   * @default Size.mebibytes(3)
   */
  readonly bufferingSize?: cdk.Size;

  /**
   * The compression that Firehose uses when delivering data to the intermediate bucket.
   *
   * @default Compression.UNCOMPRESSED
   */
  readonly compression?: firehose.Compression;
}

/**
 * Redshift delivery stream destination.
 */
export class RedshiftDestination extends firehose.DestinationBase {
  protected readonly redshiftProps: RedshiftDestinationProps;

  /**
   * The secret Firehose will use to access the Redshift cluster.
   */
  public secret?: secretsmanager.ISecret;

  private masterSecret: secretsmanager.ISecret;

  constructor(redshiftProps: RedshiftDestinationProps) {
    super(redshiftProps);

    this.redshiftProps = redshiftProps;

    if (redshiftProps.backup === firehose.BackupMode.FAILED) {
      throw new Error(`Redshift delivery stream destination only supports ENABLED and DISABLED BackupMode, given ${firehose.BackupMode[redshiftProps.backup]}`);
    }
    const cluster = redshiftProps.cluster;
    if (!cluster.publiclyAccessible) {
      throw new Error('Redshift cluster used as Firehose destination must be publicly accessible');
    }
    if (!cluster.subnetGroup?.selectedSubnets?.hasPublic) {
      throw new Error('Redshift cluster used as Firehose destination must be located in a public subnet');
    }
    const masterSecret = redshiftProps.masterSecret ?? (cluster instanceof redshift.Cluster ? cluster.secret : undefined);
    if (!masterSecret) {
      throw new Error('Master secret must be provided or Redshift cluster must generate a master secret');
    }
    this.masterSecret = masterSecret;
    if (redshiftProps.retryTimeout && redshiftProps.retryTimeout.toSeconds() > cdk.Duration.hours(2).toSeconds()) {
      throw new Error('Retry timeout must be less that 2 hours');
    }
    if (redshiftProps.compression === firehose.Compression.SNAPPY || redshiftProps.compression === firehose.Compression.ZIP) {
      throw new Error('Compression must not be SNAPPY or ZIP');
    }
  }

  public bind(scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
    return {
      properties: {
        redshiftDestinationConfiguration: this.createRedshiftConfig(scope, options.deliveryStream),
      },
    };
  }

  private createRedshiftConfig(
    scope: Construct,
    deliveryStream: firehose.IDeliveryStream,
  ): firehose.CfnDeliveryStream.RedshiftDestinationConfigurationProperty {
    const {
      cluster,
      database,
      tableName,
      tableColumns,
      user: userConfig,
      compression,
      retryTimeout,
      bufferingInterval,
      bufferingSize,
      copyOptions,
      backup,
    } = this.redshiftProps;

    const endpoint = cluster.clusterEndpoint;
    const jdbcUrl = `jdbc:redshift://${endpoint.hostname}:${cdk.Token.asString(endpoint.port)}/${database}`;
    cluster.connections.allowDefaultPortFrom(deliveryStream, 'Allow incoming connections from Kinesis Data Firehose');

    const redshiftTable = new FirehoseRedshiftTable(scope, 'Firehose Redshift Table', {
      cluster,
      masterSecret: this.masterSecret,
      database: database,
      tableName: tableName,
      tableColumns: tableColumns,
    });

    const user = (() => {
      if (userConfig.password) {
        return {
          username: userConfig.username,
          password: userConfig.password,
        };
      } else {
        const secret = new redshift.DatabaseSecret(scope, 'Firehose User Secret', {
          username: userConfig.username,
          encryptionKey: userConfig.encryptionKey,
        });
        this.secret = secret.attach(cluster);

        const redshiftUser = new FirehoseRedshiftUser(scope, 'Firehose Redshift User', {
          cluster,
          masterSecret: this.masterSecret,
          userSecret: this.secret,
          database: database,
          tableName: redshiftTable.tableName,
        });

        return {
          username: redshiftUser.username,
          password: this.secret.secretValueFromJson('password'),
        };
      };
    })();

    const intermediateBucket = this.redshiftProps.intermediateBucket ?? new s3.Bucket(scope, 'Intermediate Bucket');
    intermediateBucket.grantReadWrite(deliveryStream);
    const intermediateS3Config: firehose.CfnDeliveryStream.S3DestinationConfigurationProperty = {
      bucketArn: intermediateBucket.bucketArn,
      roleArn: (deliveryStream.grantPrincipal as iam.Role).roleArn,
      bufferingHints: this.createBufferingHints(bufferingInterval, bufferingSize),
      cloudWatchLoggingOptions: this.createLoggingOptions(scope, deliveryStream, 'IntermediateS3'),
      compressionFormat: compression ?? firehose.Compression.UNCOMPRESSED,
    };
    // TODO: encryptionConfiguration? why need to provide if bucket has encryption
    const bucketAccessRole = this.redshiftProps.bucketAccessRole ?? new iam.Role(scope, 'Intermediate Bucket Access Role', {
      assumedBy: new iam.ServicePrincipal('redshift.amazonaws.com'),
    });
    if (!this.redshiftProps.bucketAccessRole && cluster instanceof redshift.Cluster) {
      cluster.attachRole(bucketAccessRole);
    }
    intermediateBucket.grantRead(bucketAccessRole);

    return {
      clusterJdbcurl: jdbcUrl,
      copyCommand: {
        dataTableName: tableName,
        dataTableColumns: tableColumns.map(column => column.name).join(),
        copyOptions: copyOptions,
      },
      password: user.password.toString(),
      username: user.username.toString(),
      s3Configuration: intermediateS3Config,
      roleArn: (deliveryStream.grantPrincipal as iam.Role).roleArn,
      cloudWatchLoggingOptions: this.createLoggingOptions(scope, deliveryStream, 'Redshift'),
      processingConfiguration: this.createProcessingConfig(deliveryStream),
      retryOptions: this.createRetryOptions(retryTimeout),
      s3BackupConfiguration: this.createBackupConfig(scope, deliveryStream),
      s3BackupMode: (backup === firehose.BackupMode.ALL) ? 'Enabled' : 'Disabled',
    };
  }

  private createRetryOptions(retryTimeout?: cdk.Duration): firehose.CfnDeliveryStream.RedshiftRetryOptionsProperty | undefined {
    return retryTimeout ? {
      durationInSeconds: retryTimeout.toSeconds(),
    } : undefined;
  }
}
