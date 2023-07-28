import boto3
iam = boto3.resource('iam')

policy_arm = "arn:aws:iam::aws:policy/CloudFrontFullAccess"
def call(pcy, version=None):
    if version:
        ver = iam.PolicyVersion(pcy,version)
    else:
        policy = iam.Policy(pcy)
        ver = policy.default_version
        
    return ver.document

body = call(policy_arm)
print(body)
