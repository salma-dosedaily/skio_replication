# Skio Replication Pipeline — Infrastructure Diagram

```mermaid
flowchart TB
    %% Styles
    classDef default fill:#fff,stroke:#333,stroke-width:2px,color:#333;
    classDef cloud fill:#e0f7fa,stroke:#006064,stroke-width:2px,color:#006064;
    classDef config fill:#ffecb3,stroke:#ff6f00,stroke-width:2px,color:#ff6f00;
    classDef process fill:#fff9c4,stroke:#f57f17,stroke-width:2px,color:#f57f17;
    classDef external fill:#e1bee7,stroke:#4a148c,stroke-width:2px,color:#4a148c;
    classDef alert fill:#ffccbc,stroke:#d84315,stroke-width:2px,color:#d84315;

    %% Orchestration
    subgraph Orchestration["Orchestration"]
        CS["Cloud Scheduler - Cron 6x/day"]:::cloud
        CF["Cloud Function - replicate_skio_data"]:::cloud
        CS -->|HTTP Trigger| CF
    end

    %% Configuration
    subgraph Config["Configuration"]
        YAML["skio_schema_with_keys.yml"]:::config
        ENV["Env Vars: SOURCE_PROJECT, GCS_BUCKET, etc"]:::config
    end

    %% Main Script
    subgraph Main["Main Script: replicate_skio_tables.py"]
        INIT["1. Initialize: BQ clients, QualityChecker, GCS"]:::process
        LOAD_SCHEMA["2. Load Schema from YAML"]:::process
        ENSURE_DS["3. Ensure Dataset exists"]:::process
        EXTRACT["4a. EXTRACT BQ to GCS AVRO"]:::process
        GCS_CLEAN["4b. Delete old blobs"]:::process
        LOAD_STG["4c. LOAD Staging from GCS"]:::process
        MERGE["4d. MERGE Staging to Final table"]:::process
        DQ_CHECK["4e. Data Quality Check"]:::alert
        INIT --> LOAD_SCHEMA --> ENSURE_DS --> EXTRACT
        EXTRACT --> GCS_CLEAN --> LOAD_STG --> MERGE --> DQ_CHECK
    end

    %% Data Quality
    subgraph DQ["data_quality.py"]
        QC["QualityChecker"]:::process
        QC_RUN["run_check: dup count, rows, freshness"]:::alert
        QC_GET["get_results"]:::alert
        QC --> QC_RUN --> QC_GET
    end

    %% Slack Reporting
    subgraph Slack["slack_reporter.py"]
        SR["SlackReporter"]:::process
        SEND["send_report"]:::alert
        SR --> SEND
    end

    %% GCP Services
    subgraph GCP["GCP Services"]
        BQ_SRC["BigQuery Source"]:::cloud
        BQ_DEST["BigQuery Dest"]:::cloud
        GCS_BUCKET["GCS Bucket"]:::cloud
        SM["Secret Manager"]:::cloud
    end

    %% External
    subgraph External["External"]
        SLACK["Slack Webhook"]:::external
    end

    %% Connections
    CF --> INIT
    YAML --> LOAD_SCHEMA
    ENV --> INIT
    DQ_CHECK --> QC_RUN
    QC_GET --> SEND
    SEND --> SLACK

    BQ_SRC -.->|Extract| EXTRACT
    GCS_BUCKET -.->|AVRO| LOAD_STG
    BQ_DEST -.->|Final| MERGE
    SM -.->|Webhook URL| SR
```

---

## Sequence Diagram: End-to-End Execution Flow

```mermaid
sequenceDiagram
    participant CS as Cloud Scheduler
    participant CF as Cloud Function
    participant BQ_SRC as BigQuery Source
    participant GCS as GCS Bucket
    participant BQ_DEST as BigQuery Dest
    participant QC as QualityChecker
    participant SM as Secret Manager
    participant SR as SlackReporter
    participant Slack as Slack

    CS->>CF: HTTP GET (dry_run=optional)
    CF->>CF: Load YAML schema
    CF->>QC: new QualityChecker(client)

    loop For each table
        CF->>BQ_SRC: EXTRACT table → GCS (AVRO)
        CF->>GCS: Delete old blobs
        CF->>BQ_DEST: LOAD from GCS → table_staging
        CF->>BQ_DEST: MERGE staging → final
        CF->>QC: run_check(table, pk, created, updated)
        QC->>BQ_DEST: Query (dup count, freshness)
        QC->>QC: Append to results[]
    end

    CF->>SM: get_secret(SLACK_WEBHOOK_URL)
    CF->>QC: get_results()
    CF->>SR: send_report(sync_results, dq_report)
    SR->>Slack: POST blocks (sync errors + DQ table)
    CF-->>CS: Return "Done. Success: N, Errors: M"
```
---

## Database model

[Live view of the model on mermaid live](https://mermaid.live/edit#pako:eNrFWFtv4jgU_itWpJmn0gVK2y1vLLRSta22gumMtOLFTQ7Bi2NnbaedTNv_vsfOhQAGMt2VloeqOZz7-c4lvAahjCAYBp1OZy5CKRYsHs4FIZzmMjNDAnw1F-5LUBNGY0UT-zUhnz6RsVRAroVhhoEuqDODtIWSwjxqUOTtrdORr2SWPelQsdQwKciQzIMl1fPgkMQfKsIHy5pyGsIR7lEUKdDa8csXseZmBvb7ILXxck4o4_kojhXE1MAX-sTBCdCKVEvVmdhQPgGDGqqENL_xuHLHRKEdc49iDd-bgu-y03l7Iw80T0CYezBLGTmpTINfojT1G-OciXhkDCSpqXJPlkxjInO_rbfCluQszJ3EU6GEpI7UUigCzp5B5QelirCa5dNLlmpi5E6KC0TcsQWEecihoBfEJmZsPm8x2OM53UFaDIhxW16iIMyUwpArwa00ln6vRdH9jBtNmNjx-0HJKAsNuadpajXuOOIQUOWvYP5KFaPClKoXoEA0emAzzlLSiympWMyECwl7JqkUbFnZtF1UHLgUsbcOI0F5blioyWcyBYNwRIv701uzTzIhMP5vTETyxRlJpGCIwoPVGVOMnN9w-fK1P0OMVM0bLjMlCDyjeU2e-y1VnO1XcXZIxSiLmLmTsZPjMj42jqqYS11PoG6kGsulVEV6jaLhCqISLl5VrwXVfrSxUCQsIg-_71AhwVmzQ8WhaRZSJbcRWpstZcoWObmdVG6_z30Va2tSG2oyvSZHCDDDEiACvpuyUyZIW3MsuKSGuITPQrs05sGE6VC6-XCDiGi0mv38pdEdxz5lejUFis_6mBBDKKOHQFdjmeH_R9jrYJppx3zd7AacNseun6UckcX887NUA_EQj51-dkqUE7HBVJesGDqta9Uo8JbJoihGGsofFAvBU9AQs2kgGpn2afNCyw2koy4XDqXWl28MU50ZV8AWtbSllzYvemr7QdRztl0mqjJvjMUtHmvi74zaYyffinRrOfwHpSk5XEh7gOJ60Db343h8PZv9cjO6vXucXnsQDkrZ4RMd7bq67E8bAa3rX0e8eYe0DdjkKewQOdVmsKW-ugfaKg7rmjSJFjhql_6DpR9Ac3nYtPUI0QLqmfJN_FRUN5-2DZTbt3UymamOoG0d1V7_gKpmTlaZtzV946g-RtqaPIDsn-xCr3d7usy56Sluvd5bL117NHzxwdl9g8uPfqDdj6PQdw219Vm5LXp0mR_21ufJ2f_ryZ4D819O4V31vlvu9UPnROiE1xdSbcv34ukxga-qW4rt5HZ_vNt9akGZVcaCkyBWLAqGRmVwEiSgcGPiY-As4W26hATmgXuFo2plF4SVSan4U8qkElMyi5fBcEG5xqcstcbLnwdqFhDY5G7WBcMrpyEYvgbfg-FgcHXavRpcdK9-vez1u93zkyAPhuf900G3e9kf9Hv93sX7SfDD2eud9s57lxe9_tXZoNfrnw9QFUT2xeG--OXC_YDx_g8ym2cv)

```mermaid
---
config:
  layout: elk
---
erDiagram
    %% Core Entities
    StorefrontUser ||--o{ Subscription : "has"
    StorefrontUser ||--o{ Order : "places"
    StorefrontUser ||--o{ Address : "owns"
    Site ||--o{ Subscription : "hosts"
    Site ||--o{ DailyAggregateTable : "aggregates"
    
    %% Subscription Details
    Subscription ||--o{ SubscriptionLine : "contains"
    Subscription }o--|| PaymentMethod : "uses"
    Subscription ||--o{ BillingAttempt : "has history"
    Subscription }|--|| Policy : "billing policy"
    Subscription }|--|| Policy : "delivery policy"
    Subscription }o--|| Address : "ships to"
    
    %% Order Lifecycle
    Order ||--o{ OrderLineItem : "contains"
    Subscription ||--o{ Order : "generates recurring"
    BillingAttempt }o--|| Order : "results in"
    
    %% Product Mapping
    SubscriptionLine }|--|| ProductVariant : "references"
    OrderLineItem }|--|| SubscriptionLine : "originates from"
    ProductVariant }|--|| Product : "belongs to"
    
    %% Analytics & Retention
    Subscription ||--o{ AnalyticsDunningWindow : "monitors"
    Subscription ||--o{ CancelFlowV2Session : "churn events v2"
    Subscription ||--o{ CancelFlowV3Session : "churn events v3"
    Subscription ||--o{ AuditLog : "logs"
    StorefrontUser ||--o{ AnalyticsSubscriberForCohort : "tracked in"

    StorefrontUser {
        string id PK
        string email
        string platformId "Shopify ID"
    }

    Subscription {
        string id PK
        string status
        datetime nextBillingDate
        float churnScore "Discovery Finding"
        json churnRiskReasons "Discovery Finding"
        int streakCount "Discovery Finding"
        string storefrontUserId FK
        string paymentMethodId FK
        string billingPolicyId FK
        string deliveryPolicyId FK
        string shippingAddressId FK
    }

    Order {
        string id PK
        string subscriptionId FK
        float totalPrice
        datetime createdAt
        string storefrontUserId FK
    }

    SubscriptionLine {
        string id PK
        float priceWithoutDiscount "Discovery Finding"
        int ordersRemaining
        string subscriptionId FK
        string productVariantId FK
        int quantity
    }

    BillingAttempt {
        string id PK
        string subscriptionId FK
        string orderId FK
        string status "SUCCESS/FAILURE"
        string errorCode "Discovery Finding"
        datetime billingAttemptedAt
    }

    PaymentMethod {
        string id PK
        string type
        string last4
    }

    Address {
        string id PK
        string city
        string country
        string zip
        string storefrontUserId FK
    }

    Policy {
        string id PK
        string interval
        int intervalCount
    }

    Product {
        string id PK
        string title
    }

    ProductVariant {
        string id PK
        string title
        string sku
        float price
    }

    OrderLineItem {
        string id PK
        string orderId FK
        string productVariantId FK
        int quantity
        float price
        string subscriptionLineId FK
    }

    AuditLog {
        string id PK
        string eventType
        string eventData
        string subscriptionId FK
        string storefrontUserId FK
    }

    CancelFlowV2Session {
        string id PK
        string reason
        string status
        string subscriptionId FK
    }

    CancelFlowV3Session {
        string id PK
        string reason
        string status
        string subscriptionId FK
    }

    AnalyticsDunningWindow {
        string id PK
        string subscriptionId FK
    }

    AnalyticsSubscriberForCohort {
        string storefrontUserId FK
        string cohortDate
    }

    DailyAggregateTable {
        string siteId FK
        date date
        float totalRevenue
    }
```