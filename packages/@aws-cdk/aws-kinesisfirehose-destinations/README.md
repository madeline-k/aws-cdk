# Amazon Kinesis Firehose Destinations Library
<!--BEGIN STABILITY BANNER-->

---

![cdk-constructs: Experimental](https://img.shields.io/badge/cdk--constructs-experimental-important.svg?style=for-the-badge)

> The APIs of higher level constructs in this module are experimental and under active development.
> They are subject to non-backward compatible changes or removal in any future version. These are
> not subject to the [Semantic Versioning](https://semver.org/) model and breaking changes will be
> announced in the release notes. This means that while you may use them, you may need to update
> your source code when upgrading to a newer version of this package.

---

<!--END STABILITY BANNER-->

This library provides constructs for adding destinations to a Kinesis Firehose delivery stream.
Destinations can be added by specifying the `destination` prop when creating a delivery stream.

## Destinations

The following destinations are supported

* Elasticsearch

Example with an Elasticsearch destination:

``` typescript
import * as es from '@aws-cdk/aws-elasticsearch';
import * as firehose from '@aws-cdk/aws-kinesisfirehose';
import * as destinations from '@aws-cdk/aws-kinesisfirehose-destinations';

const myDomain = new es.Domain(this, 'Domain', {
  version: es.ElasticsearchVersion.V7_1,
});

const deliveryStream = new firehose.DeliveryStream(this, 'DeliveryStream', {
  destination: new destinations.ElasticsearchDestination({
    domain: devDomain,
    indexName: 'myindex',
  }),
});
```
