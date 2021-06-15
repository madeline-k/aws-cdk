import '@aws-cdk/assert-internal/jest';
import * as ec2 from '@aws-cdk/aws-ec2';
import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as lambda from '@aws-cdk/aws-lambda';
import * as redshift from '@aws-cdk/aws-redshift';
import * as s3 from '@aws-cdk/aws-s3';
import * as secretsmanager from '@aws-cdk/aws-secretsmanager';
import * as cdk from '@aws-cdk/core';
import * as firehosedestinations from '../lib';

describe('redshift destination', () => {
  let stack: cdk.Stack;
  let vpc: ec2.Vpc;
  let cluster: redshift.ICluster;
  const deliveryStreamRoleArn = 'arn:aws:iam::111122223333:role/my-role';
  let deliveryStreamRole: iam.IRole;
  let deliveryStream: firehose.IDeliveryStream;

  beforeEach(() => {
    stack = new cdk.Stack();
    vpc = new ec2.Vpc(stack, 'VPC');
    cluster = new redshift.Cluster(stack, 'Cluster', {
      vpc: vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      masterUser: {
        masterUsername: 'master',
      },
      publiclyAccessible: true,
    });
    deliveryStreamRole = iam.Role.fromRoleArn(stack, 'Delivery Stream Role', deliveryStreamRoleArn);
    deliveryStream = firehose.DeliveryStream.fromDeliveryStreamAttributes(stack, 'Delivery Stream', {
      deliveryStreamName: 'mydeliverystream',
      role: deliveryStreamRole,
    });
  });

  test('produces config when minimally specified', () => {
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
    });

    const destinationConfig = destination.bind(stack, { deliveryStream });

    expect(stack.resolve(destinationConfig)).toStrictEqual({
      properties: {
        redshiftDestinationConfiguration: {
          clusterJdbcurl: {
            'Fn::Join': [
              '',
              [
                'jdbc:redshift://',
                {
                  'Fn::GetAtt': [
                    'ClusterEB0386A7',
                    'Endpoint.Address',
                  ],
                },
                ':',
                {
                  'Fn::GetAtt': [
                    'ClusterEB0386A7',
                    'Endpoint.Port',
                  ],
                },
                '/database',
              ],
            ],
          },
          copyCommand: {
            dataTableName: 'tableName',
            dataTableColumns: 'col1,col2',
          },
          username: {
            Ref: 'FirehoseRedshiftUser',
          },
          password: {
            'Fn::Join': [
              '',
              [
                '{{resolve:secretsmanager:',
                { Ref: 'FirehoseUserSecretAttachment159E4C5F' },
                ':SecretString:password::}}',
              ],
            ],
          },
          roleArn: deliveryStreamRoleArn,
          s3Configuration: {
            bucketArn: { 'Fn::GetAtt': ['IntermediateBucketA26E6E08', 'Arn'] },
            roleArn: deliveryStreamRoleArn,
            compressionFormat: 'UNCOMPRESSED',
          },
          s3BackupMode: 'Disabled',
        },
      },
    });
  });

  test('produces config when fully specified', () => {
    const processorFunctionArn = 'arn:aws:lambda:xx-west-1:111122223333:function:my-function';
    const processorFunction = lambda.Function.fromFunctionAttributes(stack, 'Processor', {
      functionArn: processorFunctionArn,
      sameEnvironment: true,
    });
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      copyOptions: 'json \'auto\'',
      retryTimeout: cdk.Duration.minutes(2),
      bufferingInterval: cdk.Duration.minutes(1),
      bufferingSize: cdk.Size.mebibytes(1),
      compression: firehose.Compression.GZIP,
      logging: true,
      processors: [{ lambdaFunction: processorFunction }],
      backup: firehose.BackupMode.ENABLED,
    });

    const destinationConfig = destination.bind(stack, { deliveryStream });

    expect(stack.resolve(destinationConfig)).toStrictEqual(stack.resolve({
      properties: {
        redshiftDestinationConfiguration: {
          clusterJdbcurl: {
            'Fn::Join': [
              '',
              [
                'jdbc:redshift://',
                {
                  'Fn::GetAtt': [
                    'ClusterEB0386A7',
                    'Endpoint.Address',
                  ],
                },
                ':',
                {
                  'Fn::GetAtt': [
                    'ClusterEB0386A7',
                    'Endpoint.Port',
                  ],
                },
                '/database',
              ],
            ],
          },
          copyCommand: {
            copyOptions: 'json \'auto\'',
            dataTableName: 'tableName',
            dataTableColumns: 'col1,col2',
          },
          username: {
            Ref: 'FirehoseRedshiftUser',
          },
          password: {
            'Fn::Join': [
              '',
              [
                '{{resolve:secretsmanager:',
                { Ref: 'FirehoseUserSecretAttachment159E4C5F' },
                ':SecretString:password::}}',
              ],
            ],
          },
          roleArn: deliveryStreamRoleArn,
          s3Configuration: {
            bucketArn: { 'Fn::GetAtt': ['IntermediateBucketA26E6E08', 'Arn'] },
            roleArn: deliveryStreamRoleArn,
            compressionFormat: 'GZIP',
            bufferingHints: {
              intervalInSeconds: 60,
              sizeInMBs: 1,
            },
            cloudWatchLoggingOptions: {
              enabled: true,
              logGroupName: { Ref: 'LogGroupD9735569' },
              logStreamName: { Ref: 'LogGroupIntermediateS3AD1EA93E' },
            },
          },
          s3BackupMode: 'Enabled',
          s3BackupConfiguration: {
            bucketArn: { 'Fn::GetAtt': ['BackupBucket1DE570B5', 'Arn'] },
            roleArn: deliveryStreamRoleArn,
          },
          cloudWatchLoggingOptions: {
            enabled: true,
            logGroupName: { Ref: 'LogGroupD9735569' },
            logStreamName: { Ref: 'LogGroupRedshift4ADB569B' },
          },
          processingConfiguration: {
            enabled: true,
            processors: [{
              parameters: [
                { parameterName: 'LambdaArn', parameterValue: processorFunctionArn },
                { parameterName: 'RoleArn', parameterValue: deliveryStreamRoleArn },
              ],
              type: 'Lambda',
            }],
          },
          retryOptions: {
            durationInSeconds: 120,
          },
        },
      },
    }));
  });

  test('throws error if backup mode set to FAILED_ONLY', () => {
    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
      backup: firehose.BackupMode.FAILED_ONLY,
    })).toThrowError('Redshift delivery stream destination only supports ENABLED and DISABLED BackupMode, given FAILED_ONLY');
  });

  test('uses master secret if provided', () => {
    cluster = new redshift.Cluster(stack, 'Cluster With Provided Master Secret', {
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      masterUser: {
        masterUsername: 'master',
        masterPassword: cdk.SecretValue.plainText('INSECURE_NOT_FOR_PRODUCTION'),
      },
      publiclyAccessible: true,
    });
    const destination = new firehosedestinations.RedshiftDestination({
      masterSecret: secretsmanager.Secret.fromSecretNameV2(stack, 'Imported Master Secret', 'imported-master-secret'),
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
      backup: firehose.BackupMode.ENABLED,
    });

    destination.bind(stack, { deliveryStream });

    expect(stack).toHaveResourceLike('AWS::Lambda::Function', {
      Environment: {
        Variables: {
          masterSecretArn: {
            'Fn::Join': [
              '',
              [
                'arn:',
                {
                  Ref: 'AWS::Partition',
                },
                ':secretsmanager:',
                {
                  Ref: 'AWS::Region',
                },
                ':',
                {
                  Ref: 'AWS::AccountId',
                },
                ':secret:imported-master-secret',
              ],
            ],
          },
        },
      },
    });
  });

  test('throws error if master secret not provided and cluster was provided a master password', () => {
    cluster = new redshift.Cluster(stack, 'Cluster With Provided Master Secret', {
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      masterUser: {
        masterUsername: 'master',
        masterPassword: cdk.SecretValue.plainText('INSECURE_NOT_FOR_PRODUCTION'),
      },
      publiclyAccessible: true,
    });

    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
      backup: firehose.BackupMode.ENABLED,
    })).toThrowError('Master secret must be provided or Redshift cluster must generate a master secret');
  });

  test('throws error if master secret not provided and cluster was imported', () => {
    const subnetGroup = new redshift.ClusterSubnetGroup(stack, 'Cluster Subnet Group', {
      vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      description: '',
    });
    cluster = redshift.Cluster.fromClusterAttributes(stack, 'Imported Cluster', {
      clusterName: 'imported-cluster',
      clusterEndpointAddress: 'imported-cluster.abcdefghijk.xx-west-1.redshift.amazonaws.com',
      clusterEndpointPort: 5439,
      publiclyAccessible: true,
      subnetGroup,
    });

    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
      backup: firehose.BackupMode.ENABLED,
    })).toThrowError('Master secret must be provided or Redshift cluster must generate a master secret');
  });

  test('throws error if cluster not publicly accessible', () => {
    cluster = new redshift.Cluster(stack, 'Cluster Not Publicly Accessible', {
      vpc: vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      masterUser: {
        masterUsername: 'master',
      },
      publiclyAccessible: false,
    });

    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
    })).toThrowError('Redshift cluster used as Firehose destination must be publicly accessible');
  });

  test('throws error if cluster not in public subnet', () => {
    cluster = new redshift.Cluster(stack, 'Cluster Not Publicly Accessible', {
      vpc: vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PRIVATE,
      },
      masterUser: {
        masterUsername: 'master',
      },
      publiclyAccessible: true,
    });

    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }],
    })).toThrowError('Redshift cluster used as Firehose destination must be located in a public subnet');
  });

  test('uses provided intermediate bucket and access role', () => {
    const intermediateBucket = s3.Bucket.fromBucketName(stack, 'Manual Intermediate Bucket', 'manual-intermediate-bucket');
    const intermediateBucketAccessRole = iam.Role.fromRoleArn(stack, 'Manual Intermediate Bucket Access Role', 'arn:aws:iam::111122223333:role/manual-intermediate-access-role');
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
      intermediateBucket: intermediateBucket,
      bucketAccessRole: intermediateBucketAccessRole,
    });

    const destinationConfig = destination.bind(stack, { deliveryStream });

    expect(stack.resolve(destinationConfig)).toMatchObject(stack.resolve({
      properties: {
        redshiftDestinationConfiguration: {
          s3Configuration: {
            bucketArn: {
              'Fn::Join': [
                '',
                [
                  'arn:',
                  {
                    Ref: 'AWS::Partition',
                  },
                  ':s3:::manual-intermediate-bucket',
                ],
              ],
            },
          },
        },
      },
    }));
    expect(stack).toHaveResourceLike('AWS::Redshift::Cluster', {
      IamRoles: [],
    });
    expect(stack).toHaveResourceLike('AWS::IAM::Policy', {
      PolicyDocument: {
        Statement: [
          {
            Action: [
              's3:GetObject*',
              's3:GetBucket*',
              's3:List*',
            ],
            Resource: [
              {
                'Fn::Join': [
                  '',
                  [
                    'arn:',
                    {
                      Ref: 'AWS::Partition',
                    },
                    ':s3:::manual-intermediate-bucket',
                  ],
                ],
              },
              {
                'Fn::Join': [
                  '',
                  [
                    'arn:',
                    {
                      Ref: 'AWS::Partition',
                    },
                    ':s3:::manual-intermediate-bucket/*',
                  ],
                ],
              },
            ],
          },
        ],
      },
      Roles: ['manual-intermediate-access-role'],
    });
  });

  test('uses provided cluster user password', () => {
    const clusterUserPassword = cdk.SecretValue.plainText('INSECURE_NOT_FOR_PRODUCTION');
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose', password: clusterUserPassword },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
    });

    const destinationConfig = destination.bind(stack, { deliveryStream });

    expect(stack.resolve(destinationConfig)).toMatchObject(stack.resolve({
      properties: {
        redshiftDestinationConfiguration: {
          password: 'INSECURE_NOT_FOR_PRODUCTION',
        },
      },
    }));
  });

  test('creates cluster user using custom resource', () => {
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
    });

    destination.bind(stack, { deliveryStream });

    expect(stack).toHaveResource('Custom::FirehoseRedshiftUser');
  });

  test('creates cluster table using custom resource', () => {
    const destination = new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
    });

    destination.bind(stack, { deliveryStream });

    expect(stack).toHaveResource('Custom::FirehoseRedshiftTable');
  });

  test('validates retryTimeout', () => {
    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
      retryTimeout: cdk.Duration.hours(3),
    })).toThrowError('Retry timeout must be less that 2 hours');
  });

  test('validates compression', () => {
    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
      compression: firehose.Compression.SNAPPY,
    })).toThrowError('Compression must not be SNAPPY or ZIP');
    expect(() => new firehosedestinations.RedshiftDestination({
      cluster: cluster,
      user: { username: 'firehose' },
      database: 'database',
      tableName: 'tableName',
      tableColumns: [{ name: 'col1', dataType: 'varchar(4)' }, { name: 'col2', dataType: 'float' }],
      logging: false,
      compression: firehose.Compression.ZIP,
    })).toThrowError('Compression must not be SNAPPY or ZIP');
  });
});
