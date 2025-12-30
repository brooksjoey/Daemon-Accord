from .legacy_base_action import BaseAction, ActionContext, ActionResult

class CustomAction(BaseAction):
    action_type = "custom"
    
    async def execute(self, context: ActionContext) -> ActionResult:
        # Implement custom action logic
        result = await context.page.evaluate("customJavaScript()")
        return self._create_result(
            success=True,
            context=context,
            data={"result": result}
        )

# 2. Register in action registry
# backend/app/execution/actions/__init__.py
from .custom_action import CustomAction

ACTION_REGISTRY = {
    "navigate": NavigateExtractAction,
    "authenticate": AuthenticateAction,
    "custom": CustomAction  # Added
}

# 3. Use in job configuration
# {
#   "actions": [
#     {"type": "navigate", "url": "https://example.com"},
#     {"type": "custom", "params": {...}}
#   ]
# }
