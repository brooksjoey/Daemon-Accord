import yaml
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

class WorkflowComposer:
    """Compose and manipulate workflows programmatically"""
    
    @staticmethod
    def create_workflow(name: str, description: str = "") -> Dict:
        """Create empty workflow template"""
        return {
            "name": name,
            "version": "1.0.0",
            "description": description,
            "steps": [],
            "timeout": 300,
            "priority": 1,
            "capture_artifacts": True,
            "rate_limit_delay": 2,
            "retry_on_failure": True,
            "max_retries": 3,
            "tags": [],
            "metadata": {
                "created_at": datetime.utcnow().isoformat(),
                "created_by": "workflow_composer"
            }
        }
    
    @staticmethod
    def add_navigation_step(workflow: Dict, url: str, 
                           name: Optional[str] = None,
                           wait_for: List[str] = None,
                           capture: bool = True) -> Dict:
        """Add navigation step to workflow"""
        step = {
            "type": "navigate",
            "url": url,
            "wait_for": wait_for or [],
            "capture": capture,
            "timeout": 30,
            "retry_count": 3
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_click_step(workflow: Dict, selector: str,
                      name: Optional[str] = None,
                      wait_for: List[str] = None,
                      retry_count: int = 3) -> Dict:
        """Add click step to workflow"""
        step = {
            "type": "click",
            "selector": selector,
            "wait_for": wait_for or [],
            "retry_count": retry_count,
            "timeout": 10
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_fill_step(workflow: Dict, selector: str, value: Any,
                     name: Optional[str] = None,
                     wait_for: List[str] = None) -> Dict:
        """Add fill step to workflow"""
        step = {
            "type": "fill",
            "selector": selector,
            "value": value,
            "wait_for": wait_for or [],
            "timeout": 10,
            "retry_count": 3
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_extraction_step(workflow: Dict, selector: str,
                           attribute: Optional[str] = None,
                           name: Optional[str] = None,
                           store_as: Optional[str] = None) -> Dict:
        """Add extraction step to workflow"""
        step = {
            "type": "extract",
            "selector": selector,
            "timeout": 10,
            "retry_count": 3
        }
        
        if attribute:
            step["attribute"] = attribute
        
        if name:
            step["name"] = name
        
        if store_as:
            step["store_as"] = store_as
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_javascript_step(workflow: Dict, script: str,
                           name: Optional[str] = None,
                           wait_for: List[str] = None) -> Dict:
        """Add JavaScript execution step"""
        step = {
            "type": "execute_js",
            "script": script,
            "wait_for": wait_for or [],
            "timeout": 10,
            "retry_count": 3
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_wait_step(workflow: Dict, wait_time: int = 5,
                     name: Optional[str] = None) -> Dict:
        """Add wait step to workflow"""
        step = {
            "type": "wait",
            "wait_time": wait_time,
            "timeout": wait_time + 5
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_screenshot_step(workflow: Dict,
                           name: Optional[str] = None,
                           full_page: bool = True) -> Dict:
        """Add screenshot step to workflow"""
        step = {
            "type": "screenshot",
            "full_page": full_page,
            "timeout": 10
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_conditional_step(workflow: Dict, condition: str,
                            if_steps: List[Dict],
                            else_steps: List[Dict] = None,
                            name: Optional[str] = None) -> Dict:
        """Add conditional step block"""
        step = {
            "type": "conditional",
            "condition": condition,
            "if": if_steps,
            "timeout": 30
        }
        
        if else_steps:
            step["else"] = else_steps
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def add_loop_step(workflow: Dict, collection: str,
                     steps: List[Dict],
                     name: Optional[str] = None,
                     max_iterations: int = 100) -> Dict:
        """Add loop step block"""
        step = {
            "type": "loop",
            "collection": collection,
            "steps": steps,
            "max_iterations": max_iterations,
            "timeout": 300
        }
        
        if name:
            step["name"] = name
        
        workflow["steps"].append(step)
        return workflow
    
    @staticmethod
    def set_workflow_timeout(workflow: Dict, timeout: int) -> Dict:
        """Set workflow timeout"""
        workflow["timeout"] = timeout
        return workflow
    
    @staticmethod
    def set_workflow_priority(workflow: Dict, priority: int) -> Dict:
        """Set workflow priority"""
        workflow["priority"] = priority
        return workflow
    
    @staticmethod
    def add_tags(workflow: Dict, tags: List[str]) -> Dict:
        """Add tags to workflow"""
        workflow["tags"].extend(tags)
        workflow["tags"] = list(set(workflow["tags"]))  # Remove duplicates
        return workflow
    
    @staticmethod
    def add_metadata(workflow: Dict, key: str, value: Any) -> Dict:
        """Add metadata to workflow"""
        workflow["metadata"][key] = value
        return workflow
    
    @staticmethod
    def to_yaml(workflow: Dict) -> str:
        """Convert workflow to YAML"""
        return yaml.dump(workflow, default_flow_style=False, sort_keys=False)
    
    @staticmethod
    def to_json(workflow: Dict) -> str:
        """Convert workflow to JSON"""
        return json.dumps(workflow, indent=2)
    
    @staticmethod
    def validate_structure(workflow: Dict) -> List[str]:
        """Validate workflow structure"""
        errors = []
        
        # Check required fields
        required = ["name", "steps"]
        for field in required:
            if field not in workflow:
                errors.append(f"Missing required field: {field}")
        
        if "steps" in workflow and not isinstance(workflow["steps"], list):
            errors.append("Steps must be a list")
        
        # Check step types
        valid_step_types = ["navigate", "click", "fill", "extract", 
                          "execute_js", "wait", "screenshot", "conditional", "loop"]
        
        if "steps" in workflow and isinstance(workflow["steps"], list):
            for i, step in enumerate(workflow["steps"]):
                if not isinstance(step, dict):
                    errors.append(f"Step {i} must be a dictionary")
                    continue
                
                step_type = step.get("type")
                if not step_type:
                    errors.append(f"Step {i} missing type")
                elif step_type not in valid_step_types:
                    errors.append(f"Step {i} invalid type: {step_type}")
        
        return errors
    
    @staticmethod
    def estimate_complexity(workflow: Dict) -> Dict[str, Any]:
        """Estimate workflow complexity"""
        if "steps" not in workflow:
            return {"error": "No steps in workflow"}
        
        steps = workflow["steps"]
        
        # Count step types
        step_counts = {}
        for step in steps:
            step_type = step.get("type", "unknown")
            step_counts[step_type] = step_counts.get(step_type, 0) + 1
        
        # Estimate execution time
        time_estimates = {
            "navigate": 10,
            "click": 2,
            "fill": 2,
            "extract": 3,
            "execute_js": 5,
            "wait": lambda s: s.get("wait_time", 5),
            "screenshot": 3,
            "conditional": 15,
            "loop": 30
        }
        
        total_time = 0
        for step in steps:
            step_type = step.get("type")
            if step_type in time_estimates:
                if callable(time_estimates[step_type]):
                    total_time += time_estimates[step_type](step)
                else:
                    total_time += time_estimates[step_type]
        
        # Add overhead
        total_time = total_time * 1.2
        
        return {
            "total_steps": len(steps),
            "step_counts": step_counts,
            "estimated_time_seconds": int(total_time),
            "estimated_time_minutes": int(total_time / 60),
            "timeout_ratio": total_time / workflow.get("timeout", 300),
            "has_conditionals": "conditional" in step_counts,
            "has_loops": "loop" in step_counts
        }
    
    @staticmethod
    def merge_workflows(workflow1: Dict, workflow2: Dict, 
                       strategy: str = "append") -> Dict:
        """Merge two workflows"""
        if strategy == "append":
            merged = workflow1.copy()
            merged["steps"].extend(workflow2["steps"])
            return merged
        elif strategy == "prepend":
            merged = workflow2.copy()
            merged["steps"].extend(workflow1["steps"])
            return merged
        elif strategy == "combine":
            merged = workflow1.copy()
            merged["steps"] = workflow1["steps"] + workflow2["steps"]
            
            # Merge metadata
            for key, value in workflow2.get("metadata", {}).items():
                if key not in merged["metadata"]:
                    merged["metadata"][key] = value
            
            # Merge tags
            merged["tags"] = list(set(workflow1.get("tags", []) + workflow2.get("tags", [])))
            
            # Use stricter timeout
            merged["timeout"] = max(workflow1.get("timeout", 300), 
                                   workflow2.get("timeout", 300))
            
            return merged
        else:
            raise ValueError(f"Unknown merge strategy: {strategy}")
    
    @staticmethod
    def extract_variables(workflow: Dict) -> List[str]:
        """Extract variable references from workflow"""
        import re
        
        variables = set()
        workflow_str = json.dumps(workflow)
        
        # Look for {{variable}} patterns
        pattern = r'\\{\\{([^}]+)\\}\\}'
        matches = re.findall(pattern, workflow_str)
        
        for match in matches:
            variables.add(match.strip())
        
        return sorted(list(variables))
    
    @staticmethod
    def substitute_variables(workflow: Dict, variables: Dict[str, Any]) -> Dict:
        """Substitute variables in workflow"""
        import json
        import re
        
        workflow_str = json.dumps(workflow)
        
        def replace_match(match):
            var_name = match.group(1).strip()
            if var_name in variables:
                return str(variables[var_name])
            return match.group(0)
        
        pattern = r'\\{\\{([^}]+)\\}\\}'
        substituted = re.sub(pattern, replace_match, workflow_str)
        
        return json.loads(substituted)
