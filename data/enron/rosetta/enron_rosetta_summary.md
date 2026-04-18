# Enron Rosetta Export Summary

## Event Table
- Rows: **410,019**
- Unique actors: **16,830**
- Unique thread/task IDs: **133,218**
- Escalation share: **35.07%**

### Event Type Shares
- escalation: **35.07%**
- message: **27.49%**
- assignment: **20.55%**
- message_reply: **14.73%**
- approval: **2.16%**

## Talk Graph
- Directed edges: **311,316**
- Total interactions: **2,990,873**

### Top 10 Talk Edges
- pete.davis@enron.com -> ryan.slinger@enron.com: n=9077, escalation_share=0.98
- pete.davis@enron.com -> geir.solberg@enron.com: n=9071, escalation_share=0.98
- pete.davis@enron.com -> mark.guzman@enron.com: n=9071, escalation_share=0.98
- pete.davis@enron.com -> craig.dean@enron.com: n=8285, escalation_share=0.98
- pete.davis@enron.com -> leaf.harasin@enron.com: n=5965, escalation_share=0.99
- pete.davis@enron.com -> bert.meyers@enron.com: n=5962, escalation_share=0.99
- pete.davis@enron.com -> monika.causholli@enron.com: n=5685, escalation_share=0.99
- pete.davis@enron.com -> bill.williams.iii@enron.com: n=5334, escalation_share=0.99
- pete.davis@enron.com -> dporter3@enron.com: n=5334, escalation_share=0.99
- pete.davis@enron.com -> jbryson@enron.com: n=5334, escalation_share=0.99

## Work Graph
- State transitions logged: **410,019**
- Distinct transition edges: **30**

### Top State Transitions
- escalated -> escalated: 89372 transitions across 30650 threads
- opened -> opened: 62905 transitions across 19854 threads
- assigned -> assigned: 51595 transitions across 17842 threads
- start -> opened: 44702 transitions across 44702 threads
- start -> escalated: 44646 transitions across 44646 threads
- active -> active: 32624 transitions across 11185 threads
- start -> assigned: 25294 transitions across 25294 threads
- start -> active: 16199 transitions across 16199 threads
- approved -> approved: 5816 transitions across 1858 threads
- escalated -> active: 4914 transitions across 4214 threads
