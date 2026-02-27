output "instance_public_ip" {
  description = "Public IP of the IronOraClaw instance"
  value       = oci_core_instance.ironoraclaw.public_ip
}

output "ssh_command" {
  description = "SSH into the instance"
  value       = "ssh opc@${oci_core_instance.ironoraclaw.public_ip}"
}

output "gateway_url" {
  description = "IronOraClaw gateway health endpoint"
  value       = "http://${oci_core_instance.ironoraclaw.public_ip}:8080/health"
}

output "chat_command" {
  description = "Start an interactive chat session"
  value       = "ssh opc@${oci_core_instance.ironoraclaw.public_ip} -t ironoraclaw agent"
}

output "setup_log" {
  description = "Watch the setup progress"
  value       = "ssh opc@${oci_core_instance.ironoraclaw.public_ip} -t 'tail -f /var/log/ironoraclaw-setup.log'"
}

output "oracle_password" {
  description = "Generated Oracle DB password (save this!)"
  value       = local.oracle_password
  sensitive   = true
}

output "database_mode" {
  description = "Oracle Database mode used"
  value       = local.oracle_mode
}
