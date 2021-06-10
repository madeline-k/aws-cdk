import '@aws-cdk/assert-internal/jest';
import * as cdk from '@aws-cdk/core';
import { Construct } from 'constructs';
import * as kinesisfirehose from '../lib';

describe('delivery stream', () => {
  let stack: cdk.Stack;

  beforeEach(() => {
    stack = new cdk.Stack();
  });

  test('default function', () => {
    const mockDestination: kinesisfirehose.IDestination = {
      bind(_scope: Construct, _options: kinesisfirehose.DestinationBindOptions): kinesisfirehose.DestinationConfig {
        return {
          properties: {
            mockDestinationConfig: {
              mockKey: 'mockValue',
            },
          },
        };
      },
    };

    new kinesisfirehose.DeliveryStream(stack, 'Delivery Stream', {
      destination: mockDestination,
    });

    expect(stack).toHaveResource('AWS::KinesisFirehose::DeliveryStream', {
      MockDestinationConfig: {
        MockKey: 'mockValue',
      },
    });
  });
});
