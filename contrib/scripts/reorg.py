"""
1) Run this script which will reset the node and launch it with some initial state
1) Run simple-indexer server.py
2) Type "y" into the console for the script to proceed with the reorg
"""
import subprocess
import sys
from electrumsv_sdk import commands

commands.stop(component_type='node')
commands.reset(component_type='node')
commands.start(component_type='node')

# Todo extract the transactions from blockchain_115_3677f4 and reorg them to different
#  blocks with different block hashes. That way we demonstrate that the indexer should
#  index and store all chain forks and this becomes a reproducible test with the same
#  block hashes each time we run the test.
commands.node('generate', str(1))
commands.node('generate', str(1))
commands.node('generate', str(1))


user_input = input("Are you ready for the reorg now? (y/n)")
if user_input in {'y', 'Y'}:
    print(f"Triggering reorg now...")
    process = subprocess.Popen([sys.executable, "import_blocks.py", "H:/simple-indexer/contrib/blockchains/blockchain_115_3677f4"])
    process.wait()
else:
    print(f"Reorg not performed")
