from workflows.chasm_run import chasmbot_run

jiras = ["SC-7995"]
for jira in jiras:
    try:
        chasmbot_run(jira)
    except Exception as e:
        print(f"{jira} failed: {e}")
        continue


