import * as elasticsearch from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as s3 from '@aws-cdk/aws-s3';
import { IResource, RemovalPolicy, Resource } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { CfnDeliveryStream } from './kinesisfirehose.generated';

export enum DeliveryStreamType {
  DIRECT_PUT = 'DirectPut',
}

export interface IDeliveryStream extends IResource {

}

export interface IDeliveryStreamDestination {

}

abstract class DeliveryStreamDestination implements IDeliveryStreamDestination {

}

export interface ElasticSearchDestinationProps {

  readonly domain: elasticsearch.IDomain;

  readonly indexName: string;
}

export class ElasticSearchDestination extends DeliveryStreamDestination {

  readonly domain: elasticsearch.IDomain;

  readonly indexName: string;

  constructor(props: ElasticSearchDestinationProps) {
    super();
    this.domain = props.domain;
    this.indexName = props.indexName;
  }
}

export interface DeliveryStreamProps {

  readonly deliveryStreamType: DeliveryStreamType;

  readonly destination: IDeliveryStreamDestination;
}

abstract class DeliveryStreamBase extends Resource implements IDeliveryStream {

}

export class DeliveryStream extends DeliveryStreamBase {

  constructor(scope: Construct, id: string, props: DeliveryStreamProps) {
    super(scope, id);

    const role = new iam.Role(this, 'Role', {
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    const firehoseBucket = new s3.Bucket(this, 'Bucket');

    firehoseBucket.applyRemovalPolicy(RemovalPolicy.DESTROY);
    firehoseBucket.grantReadWrite(role);

    if (props.destination instanceof ElasticSearchDestination) {
      props.destination.domain.grantReadWrite(role);

      role.addToPolicy(new iam.PolicyStatement({
        actions: [
          'es:DescribeElasticsearchDomain',
          'es:DescribeElasticsearchDomains',
          'es:DescribeElasticsearchDomainConfig',
        ],
        resources: [
          props.destination.domain.domainArn,
          props.destination.domain.domainArn + '/*',
        ],
      }));

      const deliveryStream = new CfnDeliveryStream(this, 'Resource', {
        deliveryStreamType: props.deliveryStreamType.toString(),
        elasticsearchDestinationConfiguration: {
          indexName: props.destination.indexName,
          roleArn: role.roleArn,
          s3Configuration: {
            bucketArn: firehoseBucket.bucketArn,
            roleArn: role.roleArn,
          },
          domainArn: props.destination.domain.domainArn,
        },
      });
      deliveryStream.node.addDependency(role);
    }
  }
}