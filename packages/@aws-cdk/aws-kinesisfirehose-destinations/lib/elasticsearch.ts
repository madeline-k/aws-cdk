import * as elasticsearch from '@aws-cdk/aws-elasticsearch';
import * as iam from '@aws-cdk/aws-iam';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as s3 from '@aws-cdk/aws-s3';
import { Construct } from 'constructs';

/**
 * Props for an Elasticsearch destination.
 */
export interface ElasticsearchDestinationProps extends firehose.DestinationProps {

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
export class ElasticsearchDestination extends firehose.DestinationBase {

  private readonly domain: elasticsearch.IDomain;

  private readonly indexName: string;

  constructor(props: ElasticsearchDestinationProps) {
    super(props);

    this.domain = props.domain;
    this.indexName = props.indexName;
  }

  /**
   * Returns a delivery stream destination configuration
   */
  public bind(scope: Construct, options: firehose.DestinationBindOptions): firehose.DestinationConfig {
    this.domain.grantReadWrite(options.deliveryStream);

    options.deliveryStream.grantPrincipal.addToPrincipalPolicy(new iam.PolicyStatement({
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
      roleArn: (options.deliveryStream.grantPrincipal as iam.Role).roleArn,
      s3Configuration: {
        bucketArn: (this.props.backupBucket ?? new s3.Bucket(scope, 'Backup Bucket')).bucketArn,
        roleArn: (options.deliveryStream.grantPrincipal as iam.Role).roleArn,
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
