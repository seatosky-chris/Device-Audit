##################################################################################################################
################################################  CONFIGURATION  #################################################
##################################################################################################################
### This file acts as a configuration file for the Device Audit powershell script.
### You can create a different config file for each company and then set the file to use in the DeviceAudit.ps1 script
##################################################################################################################


##################################################################################################################
##################################################  CONSTANTS  ###################################################
##################################################################################################################
### Make sure you setup the variables in this file before running the script.
### The APIKey and $OrgID are the most crucial, but you will also want to modify the AD and O365 settings based
### on the organization you are auditing.
### The contact types don't need to be changed but can be adjusted for fine tuning.
##################################################################################################################

$ReadOnly = $true # Set this to $true when testing / setting this file up to prevent the script from running auto-installs/deletions (set to $false when working properly)

$SendBillingEmails = $false # Set this to $true if this customer is billed by device (it will send an email when the bill needs to be updated)

##################################################################################################################
############################################  DATA IMPORT VARIABLES  #############################################
##################################################################################################################

####################
# $SC_CSV
#
# The path to the full machine inventory report from ScreenConnect
#
# Example: "./ScreenConnect.csv"
#
$SC_CSV =  "./ScreenConnect.csv"

####################
# $RMM_ID
#
# The ID or UID (preferred) of the company in RMM.
# You can obtain this from the companies "Settings" page in RMM
# This will only work if the Datto RMM API Key is also setup (see API Keys section). 
# As an alternative you can use the below csv import for RMM but using the ID is greatly preferred!
#
# Example: "02497-152d-439b-9a5a-d1eee27f"
#
$RMM_ID = ""

####################
# $RMM_CSV
#
# You can use a CSV export from RMM instead of the above ID (although the ID is greatly preferred!)
# When exporting, be sure to check off "Device UID", "Serial Number", "Status", "MAC Address(es)", "Last Seen", "Last User", "Warranty Expiry", and "ScreenConnect" in RMM. 
#
# Example: "./RMM.csv"
#
# $RMM_CSV =  "./RMM.csv"

####################
# $Sophos_Company
#
# The company name EXACTLY how it shows up in Sophos
#
# Example: "Sea to Sky"
#
$Sophos_Company =  ""

####################
# $SC_Company
#
# The company name on relevant devices in Screen Connect
# This can be a string for a single company name, or an array of different company names for multiple.
# In SC this info is stored in CustomProperty1
# You can use * as a wildcard
# The easiest way to get this info is to edit the company in SC then use the company strings found in the "Session Filter" section
#
# Example: @('Sea to Sky', 'STS*')
#
$SC_Company =  @()

####################
# $ITG_ID
#
# The ID of the company in IT Glue.
# This is used to update WAN / LAN overviews in ITG. $Autotask_ID must also be set.
#
# Example: "1137359"
#
$ITG_ID = ""

####################
# $Autotask_ID
#
# The ID of the company in Autotask.
# This is used to update device locations in Autotask. $ITG_ID must also be set.
#
# Example: "24567446"
#
$Autotask_ID = ""

####################
# $Azure_TenantID
#
# The TenantID of the company in Azure.
# This is used to connect Azure and Intune to the device audit.
# This only applicable if you have partner access to this tenant.
# You can use the ViewAzureTenants.ps1 script to get a full list of tenants and their ID's connected to your partner portal.
#
# Example: "aefc47c2-cf0f-6e9e-be1d-beaf5b3f2a48"
#
$Azure_TenantID = ""

####################
# $JumpCloudAPIKey
#
# Your JumpCloud API key
# Required for JumpCloud auditing to work
#
$JumpCloudAPIKey = @{
	Key = ""
}




##################################################################################################################
###############################################  COMPANY ACRONYMS  ###############################################
##################################################################################################################

# $Company_Acronym
#
# The companies acronym / initials (used for naming the exported list)
#
# Example: "STS"
#
$Company_Acronym =  ""

# $Device_Acronyms
#
# Acceptable acronym's used for naming devices 
# This will often just be 1 but for a few companies can be more. 
# These acronym's will be used to find devices assigned to the wrong company. 
# Anything not matching this list (and not a common default or differently named) will be flagged.
# You can also use ?'s as wildcards, e.g. "WS??" to match "WS01" and "WS02"
#
# Example: @("STS", "PC")
#
$Device_Acronyms =  @()

# $Device_Whitelist
#
# If there are uniquely named devices that don't match once of the above device acronyms (but are part of this organization)
# add them to this array so the script knows that they belong
#
# Example: "@("Users-Workstation")"
#
$Device_Whitelist = @()


##################################################################################################################
########################################  INSTALL / DELETE CUSTOMIZATIONS  #######################################
##################################################################################################################

# $Ignore_Installs
#
# Ignore installs on these devices.
# Under the array for the program you dont want to have installed on this device, enter the devices name or id (according to a system it currently exists in)
# e.g. For a device in SC that you don't want RMM installed on, get the devices name or id that is listed in SC, and enter this in the RMM array below  
# Alternatively, to stop installs of that program on all computers, replace the array with $true e.g. SC = $true
#
# Example: $Ignore_Installs =  @{
#	SC = @()
#	RMM = @("ENGINE-1")
#	Sophos = @("ENGINE-1")
# }
#
$Ignore_Installs =  @{
	SC = @()
	RMM = @()
	Sophos = @()
}

# $DontAutoDelete
#
# Don't auto delete these devices.
# Either put the hostname under the Hostnames array, this will block auto deletion for all devices in all systems with that hostname,
# or put the devices id or hostname under a system array (like it's done in $Ignore_Installs), and it will block auto deletions for that specific system
# This is particularly useful for EOC devices which may not turn on very often
#
# Example: $DontAutoDelete =  @{
#	Hostnames = @("EOC1")
#	SC = @()
#	RMM = @()
#	Sophos = @()
# }
#
$DontAutoDelete =  @{
	Hostnames = @()
	SC = @()
	RMM = @()
	Sophos = @()
}

# $Allow_Tamper_Protection_Disabled
#
# Allows Sophos tamper protection to be and stay disabled on these devices.
# Enter the devices name or sophos ID in the below array.
# To allow it for all devices, and a "*" as the single entry in the array.
#
# Example: $Allow_Tamper_Protection_Disabled = @(
#	"STS-1234"
# );
$Allow_Tamper_Protection_Disabled = @(
	
);


##################################################################################################################
############################################  GLOBAL CONFIG OVERWRITES  ##########################################
##################################################################################################################

####################
# $MoveCustomerList
#
# The location to move the main device list (for customers to)
#
# Example: $MoveCustomerList = @{
# 	Location = "C:\Users\Administrator\Documents\Device Audits"
#	Copy = $true
# }
#
<# $MoveCustomerList = @{
	Location = "C:\Users\Administrator\Documents\Device Audits\STS"
	Copy = $false
} #>

####################
# $MoveTechList
#
# The location to move the tech device list to
#
<# $MoveTechList = @{
	Location = "C:\Users\Administrator\Documents\Device Audits\STS"
	Copy = $false
} #>

####################
# $MoveAssetReport
#
# The location to move the asset report to
#
<# $MoveAssetReport = @{
	Location = "C:\Users\Administrator\Documents\Device Audits\STS"
	Copy = $false
} #>

##################################################################################################################
###################################################  CONSTANTS  ##################################################
##################################################################################################################

# $IgnoreSerials
#
# Default serial numbers to ignore 
# "123456789*" is also ignored on top of these
#
$IgnoreSerials =  @("To be filled by O.E.M.", "System Serial Number", "Default string")

# $InactiveDeleteDays
#
# After how many days inactive will we consider deleting a device from all services?
#
$InactiveDeleteDays =  180

# $InactiveDeleteDaysRMM
#
# After how many days inactive will we consider deleting a device from RMM? (this should be less than $InactiveDeleteDays)
#
$InactiveDeleteDaysRMM = 120

# $InactiveAutoDeleteSophos
#
# After how many days inactive will we try to autodelete a device from Sophos? 
# Be careful with this, it should have a high threshold. We don't want to delete a device that still exists and has Sophos installed.
# Set to $false to disable Sophos auto deletion entirely
#
$InactiveAutoDeleteSophos = $false

# $InactiveBillingDays
#
# After how many days inactive will we ignore a device (for billing purposes?)
#
$InactiveBillingDays = 30

# $BrokenThreshold
#
# If 2 systems last activity diverge by X days we will consider the old one to be a broken connection
#
$BrokenThreshold =  30


##################################################################################################################
#######################################  ENABLE / DISABLE SCRIPT SECTIONS  #######################################
##################################################################################################################
### These variables will allow you to enable or disable the various checks in the code
### Set any of these sections to false to not run the check
##################################################################################################################

# $DODuplicateSearch
#
# Check for duplicate devices
#
$DODuplicateSearch = $true

# $DOBrokenConnectionSearch
#
# Check for broken connections (not reporting correctly in SC, RMM, or Sophos) (based on $BrokenThreshold)
#
$DOBrokenConnectionSearch = $true

# $DOMissingConnectionSearch
#
# Check for missing connections (not in SC, RMM, or Sophos)
#
$DOMissingConnectionSearch = $true

# $DOInactiveSearch
#
# Check for inactive devices to possibly delete (based on $InactiveDeleteDays)
#
$DOInactiveSearch = $true

# $DOWrongCompanySearch
#
# Check for devices that seem like they belong under a different company (based on $Device_Acronyms)
#
$DOWrongCompanySearch = $true

# $DOUsageDBSave
#
# Save usage statistics in a sqlite database
#
$DOUsageDBSave = $true

# $DOBillingExport
#
# Export a list of devices for billing purposes (any device not seen within the last $InactiveBillingDays will be listed as unbilled)
#
$DOBillingExport = $true

# $DOUpdateDeviceLocations
#
# Update the location of devices in ITG/Autotask and update the WAN/LAN overviews
#
$DOUpdateDeviceLocations = $true


##################################################################################################################
################################################  FORCE MATCHING  ################################################
##################################################################################################################
### Force Devices to Match
### For those devices that are named poorly and just refuse to match, you can force match them from 1 system to the other by ID using the below variables
###
### In each $ForceMatch array, in the system you want to match from, create a new sub-hashtable 
### use the format:  @{"from" = "id in from system", "tosystem" = "the system to connect it to, rmm, sc, or sophos", "to" = "id in the to system, or $false for no match"}
### for rmm use the devices uid (Device information > more > ID)
###
### Example:
###   SC = @(
###   		@{
###   			# MacBook-Pro (No RMM)
###   			from = "1a5-f9e6-c394-e05c-4f5d78"
###   			tosystem = "rmm"
###   			to = $false
###   		},
###   		@{
###   			# MacBook-Pro (Sophos: MacBook Pro)
###   			from = "1a5-f9e6-c394-e05c-4f5d78"
###   			tosystem = "sophos"
###   			to = "324-53ca-c4f2-a868-981e7a5"
###   		}
###   	)
###
### See MV's configuration for a full example of this in use
##################################################################################################################

# $ForceMatch
#
# All matches to force. See instructions above for use.
#
$ForceMatch = @{
	# From SC
	SC = @(
		
	)
	# From RMM
	RMM = @(
		
	)
	# From Sophos
	Sophos = @(

	)
}