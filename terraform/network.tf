module "vpc" {
  source = "./modules/vpc"

  network_name = var.vpc_network_name
  project_id   = var.project
  description  = var.vpc_network_description
  routing_mode = var.vpc_network_routing_mode
}

module "subnet" {
  source = "./modules/subnets"

  subnet_name           = var.subnet_name
  subnet_ip             = var.subnet_range
  subnet_region         = var.compute_region
  subnet_private_access = true
  network_name          = module.vpc.network_name
  project_id            = var.project
  description           = var.subnet_description
}

module "egress_deny_all" {
  source = "./modules/firewall-rules"

  name         = var.firewall_rule_egress_deny_all_name
  description  = var.firewall_rule_egress_deny_all_description
  direction    = "EGRESS"
  network_name = module.vpc.network_name
  project_id   = var.project
  ranges       = ["0.0.0.0/0"]
  deny = [{
    protocol = "all"
  }]
}

module "egress_allow_restricted" {
  source = "./modules/firewall-rules"

  name         = var.firewall_rule_egress_allow_restricted_name
  description  = var.firewall_rule_egress_allow_restricted_description
  direction    = "EGRESS"
  network_name = module.vpc.network_name
  project_id   = var.project
  # Enables API access to Google APIs and services that are supported by VPC Service Controls.
  ranges       = ["199.36.153.4/30"]
  priority     = 999
  allow = [{
    protocol = "tcp",
    ports    = ["443"]
  }]
}

# The VPC network will use this DNS entry to be able to communicate with Google APIs (e.g. Storage)
module "googleapis_private_dns" {
  source = "./modules/cloud-dns"

  type                               = "private"
  project_id                         = var.project
  name                               = var.dns_googleapis_name
  domain                             = "googleapis.com."
  private_visibility_config_networks = [module.vpc.network_self_link]

  recordsets = [
    {
      name = "*"
      type = "CNAME"
      ttl  = 300
      records = [
        "restricted.googleapis.com."
      ]
    },
    {
      name = "restricted"
      type = "A"
      ttl  = 300
      records = [
        "199.36.153.4",
        "199.36.153.5",
        "199.36.153.6",
        "199.36.153.7"
      ]
    }
  ]
}

#module "cloud_run_private_dns" {
#  source = "./modules/cloud-dns"
#
#  type                               = "private"
#  project_id                         = var.project
#  name                               = var.dns_cloudrun_name
#  domain                             = "run.app."
#  description                        = "none"
#  private_visibility_config_networks = [module.vpc.network_self_link]
#
#  recordsets = [
#    {
#      name = "*"
#      type = "A"
#      ttl  = 300
#      records = [
#        "199.36.153.4",
#        "199.36.153.5",
#        "199.36.153.6",
#        "199.36.153.7"
#      ]
#    }
#  ]
#  depends_on = [module.googleapis_private_dns]
#}

module "vpc_connector" {
  source = "./modules/vpc-serverless-connector"

  project_id     = var.project
  name           = var.serverless_vpc_connector_name
  region         = var.compute_region
  subnet_name    = module.subnet.subnet_name
  machine_type   = var.serverless_vpc_connector_machine_type
  min_throughput = 200
  max_throughput = 1000
}