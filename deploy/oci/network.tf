resource "oci_core_vcn" "ironoraclaw" {
  compartment_id = var.compartment_ocid
  display_name   = "ironoraclaw-vcn"
  cidr_blocks    = [var.vcn_cidr]
  dns_label      = "ironvcn"
}

resource "oci_core_internet_gateway" "ironoraclaw" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.ironoraclaw.id
  display_name   = "ironoraclaw-igw"
  enabled        = true
}

resource "oci_core_route_table" "ironoraclaw" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.ironoraclaw.id
  display_name   = "ironoraclaw-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    network_entity_id = oci_core_internet_gateway.ironoraclaw.id
  }
}

resource "oci_core_security_list" "ironoraclaw" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.ironoraclaw.id
  display_name   = "ironoraclaw-sl"

  # Allow all egress
  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
    stateless   = false
  }

  # SSH
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6"
    stateless = false
    tcp_options {
      min = 22
      max = 22
    }
  }

  # Gateway API
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "6"
    stateless = false
    tcp_options {
      min = 8080
      max = 8080
    }
  }

  # ICMP
  ingress_security_rules {
    source    = "0.0.0.0/0"
    protocol  = "1"
    stateless = false
    icmp_options {
      type = 3
      code = 4
    }
  }
}

resource "oci_core_subnet" "ironoraclaw" {
  compartment_id             = var.compartment_ocid
  vcn_id                     = oci_core_vcn.ironoraclaw.id
  display_name               = "ironoraclaw-subnet"
  cidr_block                 = cidrsubnet(var.vcn_cidr, 8, 1)
  dns_label                  = "ironsub"
  route_table_id             = oci_core_route_table.ironoraclaw.id
  security_list_ids          = [oci_core_security_list.ironoraclaw.id]
  prohibit_public_ip_on_vnic = false
}
