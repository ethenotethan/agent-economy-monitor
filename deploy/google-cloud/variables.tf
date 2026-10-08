variable "project_id" {
  description = "Google Cloud project that owns the isolated runtime."
  type        = string
}

variable "billing_account" {
  description = "Billing account ID used by the mandatory project-scoped budget."
  type        = string
}

variable "region" {
  description = "Single deployment region."
  type        = string
  default     = "asia-southeast1"
}

variable "image" {
  description = "Immutable Artifact Registry image reference, preferably pinned by digest."
  type        = string

  validation {
    condition     = can(regex("@sha256:[0-9a-f]{64}$", var.image))
    error_message = "image must be pinned by sha256 digest"
  }
}

variable "namespace_id" {
  description = "Canonical tenant namespace UUID."
  type        = string
}

variable "service_secret_ids" {
  description = "Existing Secret Manager IDs for the cockpit service only."
  type        = map(string)

  validation {
    condition = toset(keys(var.service_secret_ids)) == toset([
      "DATABASE_URL",
      "COCKPIT_PASSWORD_HASH",
      "PROJECTION_GATEWAY_TOKEN",
    ])
    error_message = "service_secret_ids must contain exactly the three cockpit secret bindings"
  }
}

locals {
  required_worker_secret_names = {
    collect = toset([
      "COLLECTOR_DATABASE_URL",
      "ALCHEMY_ETHEREUM_RPC_URL",
      "ALCHEMY_BASE_RPC_URL",
      "ALCHEMY_SOLANA_RPC_URL",
      "ALCHEMY_TEMPO_RPC_URL",
    ])
    reduce = toset([
      "REDUCER_DATABASE_URL",
    ])
    verify-evidence = toset([
      "EVIDENCE_VERIFIER_DATABASE_URL",
    ])
    classify = toset([
      "CLASSIFIER_DATABASE_URL",
    ])
    enrich = toset([
      "ENRICHER_DATABASE_URL",
      "ALCHEMY_ETHEREUM_RPC_URL",
      "ALCHEMY_BASE_RPC_URL",
      "ALCHEMY_SOLANA_RPC_URL",
      "ALCHEMY_TEMPO_RPC_URL",
    ])
  }
}

variable "worker_secret_ids" {
  description = "Existing Secret Manager IDs grouped by worker mode; each job receives only its own database and RPC authority."
  type        = map(map(string))

  validation {
    condition = toset(keys(var.worker_secret_ids)) == toset(["collect", "verify-evidence", "reduce", "classify", "enrich"]) && alltrue([
      for mode, names in local.required_worker_secret_names :
      toset(keys(var.worker_secret_ids[mode])) == names
    ])
    error_message = "worker_secret_ids must contain exactly the documented mode-scoped bindings"
  }
}

variable "scheduler_schedules" {
  description = "UTC cron schedules for bounded worker jobs."
  type        = map(string)
  default = {
    collect         = "*/15 * * * *"
    verify-evidence = "2,17,32,47 * * * *"
    reduce          = "5,20,35,50 * * * *"
    classify        = "15 2 * * *"
    enrich          = "45 2 * * *"
  }
}

variable "notification_channels" {
  description = "Cloud Monitoring notification channel resource names."
  type        = list(string)

  validation {
    condition     = length(var.notification_channels) > 0
    error_message = "at least one notification channel is required for actionable alerts"
  }
}

variable "monthly_budget_amount" {
  description = "Monthly infrastructure budget in USD; external RPC, LLM, and internet egress are excluded."
  type        = number
  default     = 75

  validation {
    condition     = var.monthly_budget_amount <= 75 && var.monthly_budget_amount > 0
    error_message = "the lean runtime budget must remain in the range (0, 75] USD"
  }
}
