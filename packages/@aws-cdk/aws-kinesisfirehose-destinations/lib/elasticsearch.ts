import * as elasticsearch from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';

/**
 * Props for an Elasticsearch destination.
 */
export interface ElasticsearchDestinationProps {

  /**
   * The Amazon Elasticsearch domain that Amazon Kinesis Data Firehose delivers data to.
   */
  readonly domain: elasticsearch.IDomain;

  /**
   * The name of the Elasticsearch index to which Kinesis Data Firehose adds data for indexing.
   */
  readonly indexName: string;
}

/**
 * Use an Elasticsearch domain as Kinesis Firehose delivery stream destination.
 */
export class ElasticsearchDestination implements firehose.IDeliveryStreamDestination {

  private readonly domain: elasticsearch.IDomain;

  private readonly indexName: string;

  constructor(props: ElasticsearchDestinationProps) {
    this.domain = props.domain;
    this.indexName = props.indexName;
  }

  /**
   * Returns a delivery stream destination configuration
   */
  public bind(options: firehose.DeliveryStreamDestinationBindOptions):
  firehose.DeliveryStreamDestinationConfig {
    this.domain.grantReadWrite(options.role);

    options.role.addToPrincipalPolicy(new iam.PolicyStatement({
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

    const destination = {
      indexName: this.indexName,
      roleArn: options.role.roleArn,
      s3Configuration: {
        bucketArn: options.bucket.bucketArn,
        roleArn: options.role.roleArn,
      },
      domainArn: this.domain.domainArn,
    };

    return {
      properties: {
        elasticsearchDestinationConfiguration: destination,
      },
    };
  }
}