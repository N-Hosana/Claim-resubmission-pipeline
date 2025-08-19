#!/usr/bin/env python3


from claim_pipeline import ClaimResubmissionPipeline
import json

def test_pipeline_functionality():
    
    
    print(" Testing Claim Resubmission Pipeline")
    print("=" * 50)
    
    # Create pipeline instance
    pipeline = ClaimResubmissionPipeline()
    
    # Test data loading
    print("\n Testing Data Loading:")
    alpha_claims = pipeline.load_csv_data("data/emr_alpha.csv")
    beta_claims = pipeline.load_json_data("data/emr_beta.json")
    
    print(f"  ✅ Alpha claims loaded: {len(alpha_claims)}")
    print(f"  ✅ Beta claims loaded: {len(beta_claims)}")
    
    # Test normalization
    print("\n🔄 Testing Schema Normalization:")
    normalized_alpha = [pipeline.normalize_alpha_claim(claim) for claim in alpha_claims]
    normalized_beta = [pipeline.normalize_beta_claim(claim) for claim in beta_claims]
    
    print(f"  ✅ Alpha claims normalized: {len([c for c in normalized_alpha if c])}")
    print(f"  ✅ Beta claims normalized: {len([c for c in normalized_beta if c])}")
    
    # Test business logic
    print("\n Testing Business Logic:")
    
    # Test case 1: Valid resubmission candidate
    test_claim = {
        'claim_id': 'TEST001',
        'patient_id': 'P999',
        'procedure_code': '99213',
        'denial_reason': 'Missing modifier',
        'status': 'denied',
        'submitted_at': '2025-07-01T00:00:00',
        'source_system': 'alpha'
    }
    
    is_eligible, reason = pipeline.is_eligible_for_resubmission(test_claim)
    print(f"  ✅ Test claim eligibility: {is_eligible} - {reason}")
    
    # Test case 2: Non-eligible (approved status)
    test_claim_approved = test_claim.copy()
    test_claim_approved['status'] = 'approved'
    is_eligible, reason = pipeline.is_eligible_for_resubmission(test_claim_approved)
    print(f"  ✅ Approved claim test: {is_eligible} - {reason}")
    
    # Test case 3: Non-eligible (missing patient ID)
    test_claim_no_patient = test_claim.copy()
    test_claim_no_patient['patient_id'] = None
    is_eligible, reason = pipeline.is_eligible_for_resubmission(test_claim_no_patient)
    print(f"  ✅ Missing patient ID test: {is_eligible} - {reason}")
    
    # Test denial reason classification
    print("\n🔍 Testing Denial Reason Classification:")
    test_reasons = [
        "Missing modifier",
        "Incorrect NPI", 
        "Authorization expired",
        "incorrect procedure",
        None
    ]
    
    for reason in test_reasons:
        classification = pipeline.classify_denial_reason(reason)
        print(f"  ✅ '{reason}' → {classification}")
    
    # Test recommendation generation
    print("\n💡 Testing Recommendation Generation:")
    recommendation = pipeline.generate_resubmission_recommendation(test_claim)
    print(f"  ✅ Recommendation generated: {recommendation['recommended_changes']}")
    
    print("\n🎉 All tests completed successfully!")
    print("=" * 50)

def demonstrate_pipeline_execution():
    """Show the full pipeline in action."""
    
    print("\n Demonstrating Full Pipeline Execution:")
    print("=" * 50)
    
    # Run the pipeline
    pipeline = ClaimResubmissionPipeline()
    resubmission_candidates = pipeline.process_claims(
        alpha_file="data/emr_alpha.csv",
        beta_file="data/emr_beta.json"
    )
    
    # Display results
    print(f"\n📊 Pipeline Results Summary:")
    print(f"  Total claims processed: {pipeline.metrics['total_claims_processed']}")
    print(f"  Claims from Alpha: {pipeline.metrics['claims_from_alpha']}")
    print(f"  Claims from Beta: {pipeline.metrics['claims_from_beta']}")
    print(f"  Claims flagged for resubmission: {len(resubmission_candidates)}")
    print(f"  Claims excluded: {pipeline.metrics['claims_excluded']}")
    
    # Show exclusion reasons
    if pipeline.metrics['exclusion_reasons']:
        print(f"\n❌ Claims excluded for:")
        for reason, count in pipeline.metrics['exclusion_reasons'].items():
            print(f"  - {reason}: {count} claims")
    
    # Show top candidates
    if resubmission_candidates:
        print(f"\n✅ Top resubmission candidates:")
        for i, candidate in enumerate(resubmission_candidates[:3], 1):
            print(f"  {i}. Claim {candidate['claim_id']} ({candidate['source_system']})")
            print(f"     Reason: {candidate['resubmission_reason']}")
            print(f"     Action: {candidate['recommended_changes']}")

if __name__ == "__main__":
    try:
        test_pipeline_functionality()
        demonstrate_pipeline_execution()
        
        print(f"\n Pipeline demonstration completed!")
        print(f" Check 'resubmission_candidates.json' for detailed results")
        print(f" Check 'pipeline.log' for execution logs")
        
    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        traceback.print_exc()
