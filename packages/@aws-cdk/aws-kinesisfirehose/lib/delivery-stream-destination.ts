import * as elasticsearch from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import { IDeliveryStream } from './delivery-stream';

export interface IDeliveryStreamDestination {
  bind(scope: IDeliveryStream): void;
}

export interface ElasticSearchDestinationProps {

  readonly domain: elasticsearch.IDomain;

  readonly indexName: string;
}

export class ElasticSearchDestination implements IDeliveryStreamDestination {

  readonly domain: elasticsearch.IDomain;

  readonly indexName: string;

  constructor(props: ElasticSearchDestinationProps) {
    this.domain = props.domain;
    this.indexName = props.indexName;
  }

  public bind(scope: IDeliveryStream) {
    this.domain.grantReadWrite(scope.role);

    scope.role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'es:DescribeElasticsearchDomain',
        'es:DescribeElasticsearchDomains',
        'es:DescribeElasticsearchDomainConfig',
      ],
      resources: [
        this.domain.domainArn,
        this.domain.domainArn + '/*',
      ],
    }));

    scope.addElasticSearchDestination({
      indexName: this.indexName,
      roleArn: scope.role.roleArn,
      s3Configuration: {
        bucketArn: scope.bucket.bucketArn,
        roleArn: scope.role.roleArn,
      },
      domainArn: this.domain.domainArn,
    });
  }
}