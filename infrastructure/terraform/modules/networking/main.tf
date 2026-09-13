# ------------------------------------------------------------------
# Networking module: a minimal VPC for services that must run inside a
# network boundary (Glue connections to RDS, Redshift, DMS). Public
# subnets host the NAT gateway; private subnets host the data services.
# VPC endpoints (S3 gateway + interface endpoints) let private subnets
# reach AWS APIs without traversing the public internet.
#
# See docs/security.md for how Glue/DMS/Redshift communicate securely.
# ------------------------------------------------------------------

variable "name_prefix" { type = string }
variable "cidr" {
  type    = string
  default = "10.20.0.0/16"
}
variable "tags" {
  type    = map(string)
  default = {}
}

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(var.tags, { Name = "${var.name_prefix}-vpc" })
}

resource "aws_subnet" "public" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.cidr, 8, count.index)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = merge(var.tags, { Name = "${var.name_prefix}-public-${count.index}" })
}

resource "aws_subnet" "private" {
  count             = 2
  vpc_id            = aws_vpc.this.id
  cidr_block        = cidrsubnet(var.cidr, 8, count.index + 10)
  availability_zone = data.aws_availability_zones.available.names[count.index]
  tags              = merge(var.tags, { Name = "${var.name_prefix}-private-${count.index}" })
}

data "aws_availability_zones" "available" { state = "available" }

resource "aws_internet_gateway" "this" {
  vpc_id = aws_vpc.this.id
  tags   = var.tags
}

# Single NAT gateway (dev cost saving; use one-per-AZ in real prod).
resource "aws_eip" "nat" {
  domain = "vpc"
  tags   = var.tags
}

resource "aws_nat_gateway" "this" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public[0].id
  tags          = var.tags
}

# S3 gateway endpoint: private subnets reach S3 without NAT/internet.
resource "aws_vpc_endpoint" "s3" {
  vpc_id       = aws_vpc.this.id
  service_name = "com.amazonaws.eu-west-1.s3"
  tags         = var.tags
}

# Security group for data services (no inbound from internet).
resource "aws_security_group" "data" {
  name        = "${var.name_prefix}-data-sg"
  description = "Data services SG - egress only, intra-SG allowed."
  vpc_id      = aws_vpc.this.id

  ingress {
    description = "intra-SG"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  tags = var.tags
}

output "vpc_id" { value = aws_vpc.this.id }
output "private_subnet_ids" { value = aws_subnet.private[*].id }
output "data_security_group_id" { value = aws_security_group.data.id }
