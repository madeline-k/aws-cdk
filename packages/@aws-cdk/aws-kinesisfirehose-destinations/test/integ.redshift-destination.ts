import * as ec2 from '@aws-cdk/aws-ec2';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as redshift from '@aws-cdk/aws-redshift';
import { App, Duration, Size, Stack, StackProps } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { RedshiftDestination } from '../lib';

/**
 * Stack verification steps:
 * data=`echo '{"ticker_symbol":"AMZN","sector":"TECHNOLOGY","change":1.32,"price":736.83}' | base64
 * aws firehose put-record --delivery-stream-name RedshiftFirehoseDestination-FirehoseEF5AC2A2-XOQsa2bWGYUo --record "{\"Data\":\"$data\"}"
 */

export class TestStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    const vpc = new ec2.Vpc(this, 'Vpc');
    const database = 'my_db';
    const cluster = new redshift.Cluster(this, 'Cluster', {
      vpc: vpc,
      vpcSubnets: {
        subnetType: ec2.SubnetType.PUBLIC,
      },
      masterUser: {
        masterUsername: 'master',
      },
      defaultDatabaseName: database,
      publiclyAccessible: true,
    });

    const redshiftDestination = new RedshiftDestination({
      cluster: cluster,
      user: {
        username: 'firehose',
      },
      database: database,
      tableName: 'firehose_test_table',
      tableColumns: [
        { name: 'TICKER_SYMBOL', dataType: 'varchar(4)' },
        { name: 'SECTOR', dataType: 'varchar(16)' },
        { name: 'CHANGE', dataType: 'float' },
        { name: 'PRICE', dataType: 'float' },
      ],
      copyOptions: 'json \'auto\'',
      bufferingInterval: Duration.minutes(1),
      bufferingSize: Size.mebibytes(1),
    });
    new firehose.DeliveryStream(this, 'Firehose', {
      destination: redshiftDestination,
    });
  }
}

const app = new App();

new TestStack(app, 'aws-cdk-firehose-redshift-destination');

app.synth();
