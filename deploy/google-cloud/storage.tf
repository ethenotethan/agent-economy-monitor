resource "google_sql_database_instance" "postgres" {
  name             = "agent-economy-monitor"
  database_version = "POSTGRES_17"
  region           = var.region
  depends_on       = [google_project_service.required]

  deletion_protection = true

  settings {
    tier              = "db-f1-micro"
    availability_type = "ZONAL"
    disk_type         = "PD_SSD"
    disk_size         = 10
    disk_autoresize   = true

    ip_configuration {
      ipv4_enabled = true
      ssl_mode     = "ENCRYPTED_ONLY"
    }

    backup_configuration {
      enabled                        = true
      point_in_time_recovery_enabled = true
      start_time                     = "18:00"
      transaction_log_retention_days = 7

      backup_retention_settings {
        retained_backups = 14
        retention_unit   = "COUNT"
      }
    }

    maintenance_window {
      day          = 7
      hour         = 19
      update_track = "stable"
    }
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_sql_database" "application" {
  name     = "agent_economy"
  instance = google_sql_database_instance.postgres.name
}

resource "google_storage_bucket" "evidence" {
  name                        = "${var.project_id}-agent-economy-evidence"
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  depends_on                  = [google_project_service.required]

  versioning {
    enabled = true
  }

  retention_policy {
    retention_period = 31536000
    is_locked        = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_storage_bucket" "collection_inputs" {
  name                        = "${var.project_id}-agent-economy-collection-inputs"
  location                    = upper(var.region)
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = false
  depends_on                  = [google_project_service.required]

  versioning {
    enabled = true
  }

  lifecycle {
    prevent_destroy = true
  }
}

resource "google_service_account" "runtime" {
  account_id   = "agent-economy-runtime"
  display_name = "Agent Economy Monitor runtime"
}

resource "google_service_account" "worker" {
  for_each = local.worker_modes

  account_id   = "agent-economy-${each.key}"
  display_name = "Agent Economy Monitor ${each.key} worker"
}

resource "google_project_iam_member" "runtime_sql_client" {
  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.runtime.email}"
}

resource "google_project_iam_member" "worker_sql_client" {
  for_each = local.worker_modes

  project = var.project_id
  role    = "roles/cloudsql.client"
  member  = "serviceAccount:${google_service_account.worker[each.key].email}"
}

resource "google_project_iam_custom_role" "collector_evidence_writer" {
  role_id     = "agentEconomyEvidenceWriter"
  title       = "Agent Economy create-only evidence writer"
  description = "Create and list immutable evidence without reading object bodies"
  permissions = [
    "storage.objects.create",
    "storage.objects.list",
  ]
}

resource "google_storage_bucket_iam_member" "collector_evidence_writer" {
  bucket = google_storage_bucket.evidence.name
  role   = google_project_iam_custom_role.collector_evidence_writer.name
  member = "serviceAccount:${google_service_account.worker["collect"].email}"
}

resource "google_storage_bucket_iam_member" "runtime_evidence_creator" {
  for_each = toset(["enrich"])

  bucket = google_storage_bucket.evidence.name
  role   = google_project_iam_custom_role.collector_evidence_writer.name
  member = "serviceAccount:${google_service_account.worker[each.key].email}"
}

resource "google_storage_bucket_iam_member" "runtime_evidence_reader" {
  for_each = toset(["verify-evidence"])

  bucket = google_storage_bucket.evidence.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker[each.key].email}"
}

resource "google_storage_bucket_iam_member" "collector_input_reader" {
  bucket = google_storage_bucket.collection_inputs.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.worker["collect"].email}"
}

resource "google_secret_manager_secret_iam_member" "service_secret_access" {
  for_each = var.service_secret_ids

  project   = var.project_id
  secret_id = each.value
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.runtime.email}"
}

locals {
  worker_secret_bindings = merge([
    for mode, secrets in var.worker_secret_ids : {
      for name, secret_id in secrets : "${mode}:${name}" => {
        mode      = mode
        secret_id = secret_id
      }
    }
  ]...)
}

resource "google_secret_manager_secret_iam_member" "worker_secret_access" {
  for_each = local.worker_secret_bindings

  project   = var.project_id
  secret_id = each.value.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.worker[each.value.mode].email}"
}
