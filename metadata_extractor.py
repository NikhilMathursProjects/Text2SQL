import pandas
import os
import sqlite3
import json
from pathlib import Path
import numpy as np
from datetime import datetime
from typing import Dict,List,Optional,Any

import logging
logger = logging.getLogger(__name__)

class MetadataExtractor:
    def __init__(self,db_path:str='cloud_costs.db'):
        self.db_path=db_path
        self.conn=None
        self.finops_mappings = self._load_finops_standards()
    
    def create_connection(self) -> bool:
        """Create database connection"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            logger.info(f"Connected to database: {self.db_path}")
            return True
        except sqlite3.Error as e:
            logger.error(f"Database connection failed: {e}")
            return False
    
    def _load_finops_standards(self) -> Dict[str, Any]:
        """Load FinOps FOCUS standard definitions"""
        return {
            #Cost related field
            "effectivecost": {
                "category": "Metric",
                "description": "Amortized cost accounting for all discounts and prepayments. Represents the true cost of ownership.",
                "aggregation": "SUM",
                "unit": "Currency",
                "finops_category": "Cost",
                "expert_guidance": "Use SUM(effectivecost) for total spend analysis. This is the preferred metric for FinOps reporting."
            },
            "billedcost": {
                "category": "Metric", 
                "description": "The cost as billed by the cloud provider before any discounts or credits.",
                "aggregation": "SUM",
                "unit": "Currency",
                "finops_category": "Cost"
            },
            "billingaccountid":{
                "category":"Dimension",
                "description":"The identifier assigned to a billing account by the provider.",
                "aggregation":"COUNT DISTINCT",
                "finops_category":"Account",
            },
            "cost": {
                "category": "Metric",
                "description": "The cost amount for cloud service usage.",
                "aggregation": "SUM", 
                "unit": "Currency",
                "finops_category": "Cost"
            },
            
            # Usage fields
            "usageamount": {
                "category": "Metric",
                "description": "The quantity of service consumed. Represents raw usage before pricing.",
                "aggregation": "SUM",
                "unit": "Varies by service",
                "finops_category": "Usage"
            },
            "usagequantity": {
                "category": "Metric",
                "description": "The amount of service consumed, often in service-specific units.",
                "aggregation": "SUM",
                "unit": "Service-specific",
                "finops_category": "Usage"
            },
            "consumedquantity": {
                "category": "Metric",
                "description": "The quantity of service consumed.",
                "aggregation": "SUM",
                "unit": "Service-specific",
                "finops_category": "Usage"
            },
            
            # Service identification
            "servicename": {
                "category": "Dimension",
                "description": "The specific cloud service being used (e.g., AmazonEC2, Virtual Machines).",
                "aggregation": "GROUP BY",
                "finops_category": "Service",
                "expert_guidance": "Use in GROUP BY to break down costs by service. Essential for service-level optimization."
            },
            "servicecategory": {
                "category": "Dimension", 
                "description": "Broader category of services (e.g., Compute, Storage, Database).",
                "aggregation": "GROUP BY",
                "finops_category": "Service"
            },
            "metercategory": {
                "category": "Dimension",
                "description": "Azure-specific service category for the meter used.",
                "aggregation": "GROUP BY", 
                "finops_category": "Service"
            },
            
            # Resource identification
            "resourceid": {
                "category": "Identifier",
                "description": "Unique identifier for the cloud resource instance.",
                "aggregation": "COUNT DISTINCT",
                "finops_category": "Resource",
                "expert_guidance": "Use COUNT(DISTINCT resourceid) to count unique resources. Not suitable for cost summation."
            },
            "resourcename": {
                "category": "Dimension",
                "description": "Human-readable name of the cloud resource.",
                "aggregation": "GROUP BY",
                "finops_category": "Resource"
            },
            "resourcetype": {
                "category": "Dimension",
                "description": "Type of cloud resource (e.g., instance, storage account, database).",
                "aggregation": "GROUP BY",
                "finops_category": "Resource"
            },
            
            # Location fields
            "region": {
                "category": "Dimension", 
                "description": "Geographic region where the resource is deployed.",
                "aggregation": "GROUP BY",
                "finops_category": "Location"
            },
            "regionname": {
                "category": "Dimension",
                "description": "Human-readable name of the geographic region.",
                "aggregation": "GROUP BY", 
                "finops_category": "Location"
            },
            "availabilityzone": {
                "category": "Dimension",
                "description": "AWS-specific isolated location within a region.",
                "aggregation": "GROUP BY",
                "finops_category": "Location"
            },
            
            # Time fields
            "billingperiodstart": {
                "category": "Time",
                "description": "Start date of the billing period for the charge.",
                "aggregation": "GROUP BY",
                "finops_category": "Time"
            },
            "billingperiodend": {
                "category": "Time", 
                "description": "End date of the billing period for the charge.",
                "aggregation": "GROUP BY",
                "finops_category": "Time"
            },
            "chargeperiodstart": {
                "category": "Time",
                "description": "Start of the period when the usage occurred.",
                "aggregation": "GROUP BY",
                "finops_category": "Time"
            }
        }