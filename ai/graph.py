from langgraph.graph import END, StateGraph
from ai.core.state import AgentState
from ai.agents.triage import triage_agent
from ai.agents.investigator import investigator_agent
from ai.agents.rca_reasoner import rca_reasoner_agent
from ai.agents.responder import responder_agent

def should_investigate(state: AgentState) -> str:
    return "investigate" if state["is_incident"] else "end"

def build_graph():
    workflow = StateGraph(AgentState)
    workflow.add_node("triage", triage_agent)
    workflow.add_node("investigator", investigator_agent)
    workflow.add_node("rca", rca_reasoner_agent)
    workflow.add_node("responder", responder_agent)

    workflow.set_entry_point("triage")
    workflow.add_conditional_edges("triage", should_investigate, {"investigate": "investigator", "end": END})
    workflow.add_edge("investigator", "rca")
    workflow.add_edge("rca", "responder")
    workflow.add_edge("responder", END)

    return workflow.compile()
