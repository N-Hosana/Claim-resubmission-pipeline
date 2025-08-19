#!/usr/bin/env python3
import json
import csv
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
from pathlib import Path
import sys

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class ClaimResubmissionPipeline:
    """Main pipeline for processing claim data."""
    
    def __init__(self, reference_date: str = "2025-07-30"):
        # Set reference date for age calculations
        self.reference_date = datetime.strptime(reference_date, "%Y-%m-%d")
        
        # Known retryable reasons
        self.retryable_reasons = {
            "Missing modifier",
            "Incorrect NPI", 
            "Prior auth required"
        }
        
        # These can't be retried
        self.non_retryable_reasons = {
            "Authorization expired",
            "Incorrect provider type"
        }
        
        # Track pipeline stats
        self.metrics = {
            'total_claims_processed': 0,
            'claims_from_alpha': 0,
            'claims_from_beta': 0,
            'claims_flagged_for_resubmission': 0,
            'claims_excluded': 0,
            'exclusion_reasons': {}
        }
        
        logger.info(f"Pipeline started with reference date: {reference_date}")
    
    def load_csv_data(self, file_path: str) -> List[Dict[str, Any]]:
        """Load CSV data from Alpha system."""
        try:
            claims = []
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    claims.append(row)
            
            logger.info(f"Loaded {len(claims)} claims from CSV: {file_path}")
            return claims
            
        except FileNotFoundError:
            logger.error(f"CSV file not found: {file_path}")
            return []
        except Exception as e:
            logger.error(f"Error loading CSV: {str(e)}")
            return []
    
    def load_json_data(self, file_path: str) -> List[Dict[str, Any]]:
        """Load JSON data from Beta system."""
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                claims = json.load(file)
            
            logger.info(f"Loaded {len(claims)} claims from JSON: {file_path}")
            return claims
            
        except FileNotFoundError:
            logger.error(f"JSON file not found: {file_path}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"Error loading JSON: {str(e)}")
            return []
    
    def normalize_alpha_claim(self, claim: Dict[str, Any]) -> Dict[str, Any]:
      
        try:
            # Handle empty patient_id
            patient_id = claim.get('patient_id')
            if patient_id == '':
                patient_id = None
            
            # Fix date format
            submitted_at = claim.get('submitted_at')
            if submitted_at:
                date_obj = datetime.strptime(submitted_at, "%Y-%m-%d")
                submitted_at = date_obj.isoformat()
            
            # Clean up denial reason
            denial_reason = claim.get('denial_reason')
            if denial_reason == 'None':
                denial_reason = None
            
            return {
                'claim_id': claim.get('claim_id'),
                'patient_id': patient_id,
                'procedure_code': claim.get('procedure_code'),
                'denial_reason': denial_reason,
                'status': claim.get('status'),
                'submitted_at': submitted_at,
                'source_system': 'alpha'
            }
            
        except Exception as e:
            logger.error(f"Error normalizing Alpha claim {claim.get('claim_id', 'unknown')}: {str(e)}")
            return None
    
    def normalize_beta_claim(self, claim: Dict[str, Any]) -> Dict[str, Any]:
        
        try:
            # Map fields from Beta format to standard
            return {
                'claim_id': claim.get('id'),
                'patient_id': claim.get('member'),
                'procedure_code': claim.get('code'),
                'denial_reason': claim.get('error_msg'),
                'status': claim.get('status'),
                'submitted_at': claim.get('date'),
                'source_system': 'beta'
            }
            
        except Exception as e:
            logger.error(f"Error normalizing Beta claim {claim.get('id', 'unknown')}: {str(e)}")
            return None
    
    def classify_denial_reason(self, denial_reason: Optional[str]) -> str:
       
        if denial_reason is None:
            return 'ambiguous'
        
        # Normalize for comparison
        normalized_reason = denial_reason.strip().lower()
        
        # Check known retryable reasons
        if any(reason.lower() in normalized_reason for reason in self.retryable_reasons):
            return 'retryable'
        
        # Check known non-retryable reasons
        if any(reason.lower() in normalized_reason for reason in self.non_retryable_reasons):
            return 'non_retryable'
        
        # For ambiguous cases, use some heuristics
        ambiguous_keywords = ['incorrect', 'incomplete', 'not billable', 'form']
        if any(keyword in normalized_reason for keyword in ambiguous_keywords):
            # Assume retryable for ambiguous cases
            return 'retryable'
        
        return 'ambiguous'
    
    def is_eligible_for_resubmission(self, claim: Dict[str, Any]) -> tuple[bool, str]:
      
        try:
            # Must be denied
            if claim.get('status') != 'denied':
                return False, "Status is not denied"
            
            # Need patient ID
            if not claim.get('patient_id'):
                return False, "Missing patient ID"
            
            # Check claim age
            submitted_date = claim.get('submitted_at')
            if submitted_date:
                try:
                    # Handle different date formats
                    if 'T' in submitted_date:
                        claim_date = datetime.fromisoformat(submitted_date.replace('Z', '+00:00'))
                    else:
                        claim_date = datetime.strptime(submitted_date, "%Y-%m-%d")
                    
                    days_old = (self.reference_date - claim_date).days
                    if days_old <= 7:
                        return False, f"Claim is only {days_old} days old (need > 7 days)"
                except Exception as e:
                    logger.warning(f"Could not parse date {submitted_date}: {str(e)}")
                    return False, "Invalid date format"
            
            # Check denial reason
            denial_reason = claim.get('denial_reason')
            classification = self.classify_denial_reason(denial_reason)
            
            if classification == 'non_retryable':
                return False, f"Non-retryable denial reason: {denial_reason}"
            elif classification == 'ambiguous':
                logger.info(f"Ambiguous denial reason '{denial_reason}' classified as retryable")
            
            return True, "Eligible for resubmission"
            
        except Exception as e:
            logger.error(f"Error checking eligibility for claim {claim.get('claim_id', 'unknown')}: {str(e)}")
            return False, f"Error processing claim: {str(e)}"
    
    def generate_resubmission_recommendation(self, claim: Dict[str, Any]) -> Dict[str, Any]:
       
        denial_reason = claim.get('denial_reason', 'Unknown')
        
        # Map reasons to actions
        recommendations = {
            'Missing modifier': 'Add appropriate modifier code and resubmit',
            'Incorrect NPI': 'Review NPI number and resubmit',
            'Prior auth required': 'Obtain prior authorization and resubmit',
            'incorrect procedure': 'Review procedure code and resubmit with correct code',
            'form incomplete': 'Complete all required fields and resubmit',
            'not billable': 'Review billing requirements and resubmit if appropriate'
        }
        
        recommended_changes = recommendations.get(denial_reason, 'Review claim details and resubmit')
        
        return {
            'claim_id': claim.get('claim_id'),
            'resubmission_reason': denial_reason,
            'source_system': claim.get('source_system'),
            'recommended_changes': recommended_changes,
            'patient_id': claim.get('patient_id'),
            'procedure_code': claim.get('procedure_code'),
            'submitted_at': claim.get('submitted_at')
        }
    
    def process_claims(self, alpha_file: str, beta_file: str) -> List[Dict[str, Any]]:
        """Main processing method."""
        logger.info("Starting claim processing...")
        
        # Load data
        alpha_claims = self.load_csv_data(alpha_file)
        beta_claims = self.load_json_data(beta_file)
        
        # Update metrics
        self.metrics['claims_from_alpha'] = len(alpha_claims)
        self.metrics['claims_from_beta'] = len(beta_claims)
        self.metrics['total_claims_processed'] = len(alpha_claims) + len(beta_claims)
        
        # Normalize claims
        normalized_claims = []
        
        for claim in alpha_claims:
            normalized = self.normalize_alpha_claim(claim)
            if normalized:
                normalized_claims.append(normalized)
        
        for claim in beta_claims:
            normalized = self.normalize_beta_claim(claim)
            if normalized:
                normalized_claims.append(normalized)
        
        logger.info(f"Normalized {len(normalized_claims)} claims")
        
        # Process eligibility
        resubmission_candidates = []
        
        for claim in normalized_claims:
            is_eligible, reason = self.is_eligible_for_resubmission(claim)
            
            if is_eligible:
                recommendation = self.generate_resubmission_recommendation(claim)
                resubmission_candidates.append(recommendation)
                self.metrics['claims_flagged_for_resubmission'] += 1
                logger.info(f"Claim {claim.get('claim_id')} flagged: {reason}")
            else:
                self.metrics['claims_excluded'] += 1
                exclusion_reason = reason
                self.metrics['exclusion_reasons'][exclusion_reason] = \
                    self.metrics['exclusion_reasons'].get(exclusion_reason, 0) + 1
        
        # Log summary
        self.log_pipeline_metrics()
        
        return resubmission_candidates
    
    def log_pipeline_metrics(self):
        """Output pipeline summary."""
        logger.info("=" * 50)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total claims: {self.metrics['total_claims_processed']}")
        logger.info(f"From Alpha: {self.metrics['claims_from_alpha']}")
        logger.info(f"From Beta: {self.metrics['claims_from_beta']}")
        logger.info(f"Flagged for resubmission: {self.metrics['claims_flagged_for_resubmission']}")
        logger.info(f"Excluded: {self.metrics['claims_excluded']}")
        
        if self.metrics['exclusion_reasons']:
            logger.info("Exclusion reasons:")
            for reason, count in self.metrics['exclusion_reasons'].items():
                logger.info(f"  - {reason}: {count} claims")
        
        logger.info("=" * 50)
    
    def save_results(self, resubmission_candidates: List[Dict[str, Any]], output_file: str = "resubmission_candidates.json"):
        """Save results to file."""
        try:
            output_data = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'reference_date': self.reference_date.isoformat(),
                    'total_candidates': len(resubmission_candidates)
                },
                'candidates': resubmission_candidates
            }
            
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Results saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")


def main():
    """Main function."""
    try:
        # Start pipeline
        pipeline = ClaimResubmissionPipeline()
        
        # Process claims
        resubmission_candidates = pipeline.process_claims(
            alpha_file="data/emr_alpha.csv",
            beta_file="data/emr_beta.json"
        )
        
        # Save results
        pipeline.save_results(resubmission_candidates)
        
        # Print summary
        print(f"\n Pipeline completed!")
        print(f" Total claims: {pipeline.metrics['total_claims_processed']}")
        print(f" Eligible for resubmission: {len(resubmission_candidates)}")
        print(f" Results: resubmission_candidates.json")
        print(f" Logs: pipeline.log")
        
        if resubmission_candidates:
            print(f"\n🔍 Top candidates:")
            for i, candidate in enumerate(resubmission_candidates[:3], 1):
                print(f"  {i}. Claim {candidate['claim_id']} - {candidate['resubmission_reason']}")
        
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}")
        print(f"❌ Pipeline failed: {str(e)}")
        print("Check pipeline.log for details.")


if __name__ == "__main__":
    main()
