from datetime import datetime
with open("dernier_run.txt", "a", encoding="utf-8") as f:
    f.write(f"Run à {datetime.now()}\n")
print("OK")
