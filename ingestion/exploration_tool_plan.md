# NSW Surfing Data Source Exploration Plan

## Phase 1: GraphQL Schema Analysis & Documentation (Day 1)
- [ ] 1.1 Extract and Parse GraphQL Schema
  - Extract schema from https://docs.liveheats.com/
  - Document core types and relationships
  - Map out athlete, membership, and event hierarchies

- [ ] 1.2 Document Key Queries
  - [x] SurfingNSWClubMembership query analysis
  - [x] SurfingNSWAthletes query analysis
  - [ ] Map additional required queries for events and accounts

## Phase 2: Data Sampling & Initial Quality Assessment (Day 1-2)
- [ ] 2.1 Collect Sample Data
  ```graphql
  # Key queries to execute:
  - organisationAthletes (sample size: 100)
  - series memberships (series id: 1939, sample size: 100)
  ```

- [ ] 2.2 Initial Data Profile
  - Analyze athlete properties structure
  - Document membership status patterns
  - Map out property field usage
  - Identify required vs optional fields

## Phase 3: Data Quality Analysis (Day 2)
- [ ] 3.1 Completeness Analysis
  - Membership coverage across athletes
  - Required field population rates
  - Property field utilization

- [ ] 3.2 Consistency Checks
  - Date format standardization
  - Name field formatting
  - Property field value patterns
  - Membership status consistency

- [ ] 3.3 Business Rule Validation
  - Membership expiry logic
  - Age/DOB validation
  - Series membership rules
  - Payment status validation

## Phase 4: Data Transformation Planning (Day 3)
- [ ] 4.1 Schema Mapping
  ```python
  # Target schema structure:
  Athlete:
    - id: str
    - name: str
    - dob: date
    - properties: dict
    Membership:
    - series_id: str
    - status: str
    - expiry_date: date
    - properties: dict
  ```

- [ ] 4.2 Transformation Rules
  - Property field flattening strategy
  - Date standardization rules
  - Nested relationship handling
  - Custom field mapping

## Phase 5: Implementation & Testing (Day 3-4)
- [ ] 5.1 Development Tasks
  ```python
  # Key components to implement:
  - GraphQL client wrapper
  - Data transformation pipeline
  - Quality validation rules
  - Output formatters
  ```

- [ ] 5.2 Testing Scenarios
  - Multiple membership handling
  - Expired vs active memberships
  - Property field variations
  - Edge cases (missing data, unusual formats)

## Output Artifacts
Each phase will generate reports in:
```
ingestion/exploration_outputs/nsw_surfing/
├── schema_analysis_2025_01_31.md
├── data_quality_report_2025_01_31.md
├── transformation_rules_2025_01_31.md
└── implementation_guide_2025_01_31.md
```

## Success Criteria
- [ ] Complete schema documentation with all relevant types and relationships
- [ ] Data quality metrics baseline established
- [ ] Transformation rules documented and validated
- [ ] Sample data processed successfully through proposed pipeline
- [ ] Edge cases identified and handled
- [ ] Implementation guide ready for development team

## Next Steps
1. Begin with Phase 1 schema analysis
2. Set up GraphQL client for data sampling
3. Create initial data quality report template
4. Schedule review of findings after Phase 3
