output "cockpit_url" {
  value = google_cloud_run_v2_service.cockpit.uri
}

output "evidence_bucket" {
  value = google_storage_bucket.evidence.name
}

output "cloud_sql_connection_name" {
  value     = google_sql_database_instance.postgres.connection_name
  sensitive = true
}

output "worker_jobs" {
  value = { for mode, job in google_cloud_run_v2_job.worker : mode => job.name }
}
