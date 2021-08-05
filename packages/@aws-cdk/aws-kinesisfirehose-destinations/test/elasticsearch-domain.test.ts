import * as es from '@aws-cdk/aws-elasticsearch';
//import * as iam from '@aws-cdk/aws-iam';
import * as cdk from '@aws-cdk/core';
import * as firehosedestinations from '../lib';

describe('Elasticsearch destination', () => {
  let stack: cdk.Stack;
  let domain: es.IDomain;
  //let destinationRole: iam.IRole;

  beforeEach(() => {
    stack = new cdk.Stack();
    domain = new es.Domain(stack, 'domain', {
      version: es.ElasticsearchVersion.V7_9,
    });
    //destinationRole = new iam.Role(stack, 'Destination Role', {
    //  assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    //});
  });

  it('throws with invalid indexName', () => {
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: ',',
    })).toThrowError('Elasticsearch index name must not contain commas. indexName provided: ,');
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: '_index',
    })).toThrowError('Elasticsearch index name must not begin with an underscore. indexName provided: _index');
    expect(() => new firehosedestinations.ElasticsearchDomain(domain, {
      indexName: 'MyIndex',
    })).toThrowError('Elasticsearch index name must be lower-case. indexName provided: MyIndex');
  });

});