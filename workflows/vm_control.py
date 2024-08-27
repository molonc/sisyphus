import subprocess
import logging
import time
import argparse

# Saw it in the run_qc.sh, dont know how it work but should be the same 
log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

''' Prequsite
    ----------
    Remember to install powershell in your terminal using "sudo snap install powershell --classic" 
    and install the Az.Compute Module using the command "powershell -Command Install-Module -Name Az.Compute"
    also try to use "powershell -Command Connect-AzAccount" to connect to your azure account if you have trouble with credential
'''
def start_vm(name, resourcegroupname):
	"""
    Summary
    ----------
    To make a powershell command call "Start-AzVm" in the bash shell to start a VM that you have authorization to.
    Parameters
    ----------
    name : str
        The name of the VM you want to start
    resourcegroupname : str
        The name of reserource group the VM you want to start belongs to
    """
	log.info(f"Starting {name} in {resourcegroupname}")
	status = check_vm_status(name, resourcegroupname)

	#if Vm is already running(status = 0) or is currently starting(status = 1), we won't run the command
	if (status != 0 or status != 1 ):
		try:
			subprocess.run([
				"powershell",
				"-Command",
				"Start-AzVM",
				"-ResourceGroupName",
				f"\'{resourcegroupname}\'",
				"-Name",
				f"\'{name}\'"
				])
			while(status != 0):
				time.sleep(3)
				status = check_vm_status(name, resourcegroupname)  
		except Exception as e:
			raise Exception(f"failed to Start {name} in {resourcegroupname}: {e}")

		#since it might take some time to start the VM, the programm will keep tracking it status every 3 second until it is running

	log.info(f"Successfully started {name} in {resourcegroupname}")

def stop_vm(name, resourcegroupname):
	"""
    Summary
    ----------
    To make a powershell command call "Stop-AzVm" in the bash shell to deallocate a VM that you have authorization to.
    Parameters
    ----------
    name : str
        The name of the VM you want to deallocate
    resourcegroupname : str
        The name of reserource group the VM you want to deallocate belongs to
    """
	log.info(f"Stoping {name} in {resourcegroupname}")
	#if Vm is already Dealocated(status = 2) or is currently starting(status = 3), we won't run the command
	status = check_vm_status(name, resourcegroupname)
	if (status != 2 or status != 3 ):
		try:
			subprocess.run([
					"powershell",
					"-Command",
					"Stop-AzVM",
					"-ResourceGroupName",
					f"\'{resourcegroupname}\'",
					"-Name",
					f"\'{name}\'",
					"-Force",
					])
			while(status!=2):
				time.sleep(3)
				status = check_vm_status(name, resourcegroupname)
		except Exception as e:
			raise Exception(f"failed to Stop {name} in {resourcegroupname}: {e}")

		#since it might take some time to stop the VM, the programm will keep tracking it status every 3 second until it is deallocated
		while(status!=2):
			time.sleep(3)
			status = check_vm_status(name, resourcegroupname)

	log.info(f"Successfully stopped {name} in {resourcegroupname}")


def check_vm_status(name, resourcegroupname):
	"""
    Summary
    ----------
    To make a powershell command call "Get-AzVm" in the bash shell to check the current status of a VM that you have authorization to.
    Parameters
    ----------
    name : str
        The name of the VM you want to check
    resourcegroupname : str
        The name of reserource group the VM you want to check belongs to
    """
	log.info(f"checking status of {name} in {resourcegroupname}")
	# The programming capture the output of the command into a string and check the status of the VM
	try:
		output=subprocess.run(["powershell",
			"-Command",
			"Get-AzVM",
			"-ResourceGroupName",
			f"\'{resourcegroupname}\'",
			"-Name",
			f"\'{name}\'",
			"-Status"],capture_output=True,text=True).stdout.strip("\n")

	except Exception as e:
		raise Exception(f"failed to check status of {name} in {resourcegroupname}: {e}")

	#There should only be four VM status (running, starting, deallocating, and deallocated)
	if ("VM running" in output):
		log.info(f"{name} in {resourcegroupname} is running")
		return 0
	elif ("VM starting" in output):
		log.info(f"{name} in {resourcegroupname} is starting")
		return 1
	elif ("VM deallocated" in output):
		log.info(f"{name} in {resourcegroupname} is deallocated")
		return 2
	elif ("VM deallocating" in output):
		log.info(f"{name} in {resourcegroupname} is deallocating")
		return 3
	else:
		#run the command again to see what is the unkown status
		subprocess.run(["powershell",
			"-Command",
			"Get-AzVM",
			"-ResourceGroupName",
			f"\'{resourcegroupname}\'",
			"-Name",
			f"\'{name}\'",
			"-Status"])
		raise Exception(f"{name} in {resourcegroupname} have unknown status, please check the name of you VM and Resource Group or check your authorization")


if __name__ == "__main__":
	start_vm("bccrc-pr-loader-vm","bccrc-pr-cc-alhena-rg")
