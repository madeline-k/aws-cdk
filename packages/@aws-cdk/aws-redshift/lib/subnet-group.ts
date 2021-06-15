import * as ec2 from '@aws-cdk/aws-ec2';
import { IResource, RemovalPolicy, Resource } from '@aws-cdk/core';
import { Construct } from 'constructs';
import { CfnClusterSubnetGroup } from './redshift.generated';

/**
 * Interface for a cluster subnet group.
 */
export interface IClusterSubnetGroup extends IResource {
  /**
   * The name of the cluster subnet group.
   * @attribute
   */
  readonly clusterSubnetGroupName: string;

  /**
   * The subnets in this group.
   */
  readonly selectedSubnets?: ec2.SelectedSubnets;
}

/**
 * Properties for creating a ClusterSubnetGroup.
 */
export interface ClusterSubnetGroupProps {
  /**
   * Description of the subnet group.
   */
  readonly description: string;

  /**
   * The VPC to place the subnet group in.
   */
  readonly vpc: ec2.IVpc;

  /**
   * Which subnets within the VPC to associate with this group.
   *
   * @default - private subnets
   */
  readonly vpcSubnets?: ec2.SubnetSelection;

  /**
   * The removal policy to apply when the subnet group are removed
   * from the stack or replaced during an update.
   *
   * @default RemovalPolicy.RETAIN
   */
  readonly removalPolicy?: RemovalPolicy
}

/**
 * Class for creating a Redshift cluster subnet group
 *
 * @resource AWS::Redshift::ClusterSubnetGroup
 */
export class ClusterSubnetGroup extends Resource implements IClusterSubnetGroup {

  /**
   * Imports an existing subnet group by name.
   */
  public static fromClusterSubnetGroupName(scope: Construct, id: string, clusterSubnetGroupName: string): IClusterSubnetGroup {
    return new class extends Resource implements IClusterSubnetGroup {
      public readonly clusterSubnetGroupName = clusterSubnetGroupName;
      // TODO: allow this to be provided via from...Attributes
      public readonly selectedSubnets = undefined;
    }(scope, id);
  }

  public readonly clusterSubnetGroupName: string;
  public readonly selectedSubnets?: ec2.SelectedSubnets;

  constructor(scope: Construct, id: string, props: ClusterSubnetGroupProps) {
    super(scope, id);

    this.selectedSubnets = props.vpc.selectSubnets(props.vpcSubnets ?? { subnetType: ec2.SubnetType.PRIVATE });

    const subnetGroup = new CfnClusterSubnetGroup(this, 'Default', {
      description: props.description,
      subnetIds: this.selectedSubnets.subnetIds,
    });
    subnetGroup.applyRemovalPolicy(props.removalPolicy ?? RemovalPolicy.RETAIN, {
      applyToUpdateReplacePolicy: true,
    });

    this.clusterSubnetGroupName = subnetGroup.ref;
  }
}
