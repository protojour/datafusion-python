from datafusion import SessionContext

ctx = SessionContext()
ctx.register_memoriam_ontology(url = "https://localhost:5001", username = "testuser", password = "secret")

# df = ctx.sql('select * from ontology.stix."attack-pattern"')
df = ctx.sql('select * from ontology.stix."relationship"')
# df = ctx.sql('select "type" from ontology.stix."attack-pattern"')
# df = ctx.sql('select * from ontology.stix.url')

df
