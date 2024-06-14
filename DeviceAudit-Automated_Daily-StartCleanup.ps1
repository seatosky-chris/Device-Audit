param(
	$companies = @(),
	[switch]$ForceMonthlyUsageRollup
)
#####################################################################
### This breaks out the device cleanup/audit from the Automated device audit.
### It will run the cleanup against whichever organizations you specify.
### Be sure to run the DeviceMatching first, this will use exported device
### match json files.
###
### Run this with a single argument
### The argument should either be the company's acronym (referencing a config file)
### or "ALL" which will audit every company there is a config file for
### You can also list multiple companies to target a few specific ones
### e.g. DeviceAudit-Automated.ps1 -companies STS, AVA, MV  # (note the companies flag is optional)
###
### Make sure there is a config file for the company under the "Config Files/" folder
###

. "$PSScriptRoot\Config Files\APIKeys.ps1" # API Keys
. "$PSScriptRoot\Config Files\Global-Config.ps1" # Global Config
#####################################################################

# Setup logging
If (Get-Module -ListAvailable -Name "PSFramework") {Import-module PSFramework} Else { install-module PSFramework -Force; import-module PSFramework}
$logFile = Join-Path -path "$PSScriptRoot\ErrorLogs" -ChildPath "log-$(Get-date -f 'yyyyMMddHHmmss').txt";
Set-PSFLoggingProvider -Name logfile -FilePath $logFile -Enabled $true;

Write-PSFMessage -Level Verbose -Message "Starting cleanup on: $($companies | ConvertTo-Json)"
$CompaniesToAudit = [System.Collections.Generic.List[string]]::new();
if ($companies -contains "ALL") {
	$CompaniesToAudit = (Get-ChildItem "$PSScriptRoot\Config Files\" | Where-Object { $_.PSIsContainer -eq $false -and $_.Extension -eq '.ps1' -and $_.Name -like "Config-*" }).Name
} else {
	foreach ($CompanyConfig in $companies) {
		if ($CompanyConfig -like "Config-*") {
			$ConfigFile = $CompanyConfig
		} else {
			$ConfigFile = "Config-$CompanyConfig"
		}

		if ($ConfigFile -notlike "*.ps1") {
			$ConfigFile = "$ConfigFile.ps1"
		}

		if (Test-Path -Path "$PSScriptRoot\Config Files\$ConfigFile") {
			$CompaniesToAudit.Add($ConfigFile)
		}
	}
}

if ($CompaniesToAudit.Count -eq 0) {
	Write-Warning "No configuration files were found. Exiting!"
	Write-PSFMessage -Level Warning -Message "No configuration files found."
	if ($companies.count -eq 0) {
		Write-Warning "Please try again using the 'companies' argument.  e.g. DeviceAudit-Automated.ps1 -companies STS, AVA, MV"
	} else {
		Write-Warning "Please try again. The 'companies' argument did not seem to map to a valid config file."
	}
	exit
}

Write-Output "Computer audit starting..."

### This code is common for every company and can be ran before looping through multiple companies
$CurrentTLS = [System.Net.ServicePointManager]::SecurityProtocol
if ($CurrentTLS -notlike "*Tls12" -and $CurrentTLS -notlike "*Tls13") {
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
	Write-Output "This device is using an old version of TLS. Temporarily changed to use TLS v1.2."
	Write-PSFMessage -Level Warning -Message "Temporarily changed TLS to TLS v1.2."
}

# Import/Install any required modules
If (Get-Module -ListAvailable -Name "ImportExcel") {Import-module ImportExcel} Else { install-module ImportExcel -Force; import-module ImportExcel}
If (Get-Module -ListAvailable -Name "Az.Accounts") {Import-module Az.Accounts } Else { install-module Az.Accounts  -Force; import-module Az.Accounts }
If (Get-Module -ListAvailable -Name "Az.Resources") {Import-module Az.Resources } Else { install-module Az.Resources  -Force; import-module Az.Resources }
If (Get-Module -ListAvailable -Name "Microsoft.Graph.Authentication") {Import-module Microsoft.Graph.Authentication -Force} Else { install-module Microsoft.Graph -Force; import-module Microsoft.Graph.Authentication -Force}
If (Get-Module -ListAvailable -Name "Microsoft.Graph.Identity.DirectoryManagement") {Import-module Microsoft.Graph.Identity.DirectoryManagement -Force}
If (Get-Module -ListAvailable -Name "Microsoft.Graph.DeviceManagement") {Import-module Microsoft.Graph.DeviceManagement -Force}
If (Get-Module -ListAvailable -Name "CosmosDB") {Import-module CosmosDB } Else { install-module CosmosDB  -Force; import-module CosmosDB }
If (Get-Module -ListAvailable -Name "DattoRMM") {Import-module DattoRMM -Force} Else { install-module DattoRMM -Force; import-module DattoRMM -Force}
If (Get-Module -ListAvailable -Name "ITGlueAPI") {Import-module ITGlueAPI -Force} Else { install-module ITGlueAPI -Force; import-module ITGlueAPI -Force}
If (Get-Module -ListAvailable -Name "JumpCloud") {Import-module JumpCloud -Force} Else { install-module JumpCloud -Force; import-module JumpCloud -Force}
If (Get-Module -ListAvailable -Name "Subnet") {Import-module Subnet -Force} Else { install-module Subnet -Force; import-module Subnet -Force}

# Connect to Azure
if (Test-Path "$PSScriptRoot\Config Files\AzureServiceAccount.json") {
	$LastUpdatedAzureCreds = (Get-Item "$PSScriptRoot\Config Files\AzureServiceAccount.json").LastWriteTime
	if ($LastUpdatedAzureCreds -lt (Get-Date).AddMonths(-3)) {
		Write-PSFMessage -Level Error -Message "Azure credentials are out of date. Please run Connect-AzAccount to set up your Azure credentials."
		exit
	}

	try {
		Import-AzContext -Path "$PSScriptRoot\Config Files\AzureServiceAccount.json"
	} catch {
		Write-PSFMessage -Level Error -Message "Failed to connect to: Azure"
	}
} else {
	Connect-AzAccount
	Save-AzContext -Path "$PSScriptRoot\Config Files\AzureServiceAccount.json"
}

# Connect to IT Glue
$ITGConnected = $false
if ($ITGAPIKey.Key) {
	Add-ITGlueBaseURI -base_uri $ITGAPIKey.Url
	Add-ITGlueAPIKey $ITGAPIKey.Key
	$WANFilterID = (Get-ITGlueFlexibleAssetTypes -filter_name $WANFlexAssetName).data
	$LANFilterID = (Get-ITGlueFlexibleAssetTypes -filter_name $LANFlexAssetName).data
	$OverviewFilterID = (Get-ITGlueFlexibleAssetTypes -filter_name $OverviewFlexAssetName).data
	$ScriptsLastRunFilterID = (Get-ITGlueFlexibleAssetTypes -filter_name $ScriptsLastRunFlexAssetName).data
	$ITGConnected = $true

	if (!$WANFilterID -or !$LANFilterID -or !$OverviewFilterID) {
		Write-PSFMessage -Level Error -Message "Could not get all of the flex asset filter id's from ITG. Exiting..."
		exit 1
	}
}

# Connect to Sophos
$SophosTenantID = $false
$SophosGetTokenBody = @{
	grant_type = "client_credentials"
	client_id = $SophosAPIKey.ClientID
	client_secret = $SophosAPIKey.Secret
	scope = "token"
}
$SophosToken = Invoke-RestMethod -Method POST -Body $SophosGetTokenBody -ContentType "application/x-www-form-urlencoded" -uri "https://id.sophos.com/api/v2/oauth2/token"
$SophosJWT = $SophosToken.access_token
$SophosToken | Add-Member -NotePropertyName expiry -NotePropertyValue $null
$SophosToken.expiry = (Get-Date).AddSeconds($SophosToken.expires_in - 60)

if ($SophosJWT) {
	# Get our partner ID
	$SophosHeader = @{
		Authorization = "Bearer $SophosJWT"
	}
	$SophosPartnerInfo = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri "https://api.central.sophos.com/whoami/v1"
	$SophosPartnerID = $SophosPartnerInfo.id

	if ($SophosPartnerID) {
		# Get list of tenants, so we can get the companies ID in sophos
		$SophosHeader = @{
			Authorization = "Bearer $SophosJWT"
			"X-Partner-ID" = $SophosPartnerID
		}
		$SophosTenants = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri "https://api.central.sophos.com/partner/v1/tenants?pageTotal=true"

		if ($SophosTenants.pages -and $SophosTenants.pages.total -gt 1) {
			$TotalPages = $SophosTenants.pages.total
			for ($i = 2; $i -le $TotalPages; $i++) {
				$SophosTenants.items += (Invoke-RestMethod -Method GET -Headers $SophosHeader -uri "https://api.central.sophos.com/partner/v1/tenants?page=$i").items
			}
		} else {
			Write-PSFMessage -Level Error -Message "Failed to connect to: Sophos (No Tenants Found)"
		}
	} else {
		Write-PSFMessage -Level Error -Message "Failed to connect to: Sophos (No Partner ID)"
	}
} else {
	Write-PSFMessage -Level Error -Message "Failed to connect to: Sophos (No JWT)"
}

# Get all devices from SC
$attempt = 10
while ($attempt -ge 0) {
	if ($attempt -eq 0) {
		# Already tried 10x, lets give up and exit the script
		Write-PSFMessage -Level Error -Message "Could not get device list from ScreenConnect. Exiting..."
		exit
	}

	# Send a post request to $SCLogin.URL/Services/AuthenticationService.ashx/TryLogin
	# to set the login cookie
	$Nonce = $SC_Nonce # Manually obtained from SC.util.getRandomAlphanumericString(16);  (it just seems to care that the format is correct)
	$FormBody = @(
		$SCLogin.Username,
		$SCLogin.Password,
		$false,
		$false,
		$Nonce
	) | ConvertTo-Json

	try {
		$AuthResponse = Invoke-WebRequest "$($SCLogin.URL)/Services/AuthenticationService.ashx/TryLogin" -SessionVariable 'SCWebSession' -Body $FormBody -Method 'POST' -ContentType 'application/json'
	} catch {
		$attempt--
		Write-PSFMessage -Level Error -Message "Failed to connect to: ScreenConnect"
		Write-PSFMessage -Level Error -Message "Status Code: $($_.Exception.Response.StatusCode.Value__)"
		Write-PSFMessage -Level Error -Message "Message: $($_.Exception.Message)"
		Write-PSFMessage -Level Error -Message "Status Description: $($_.Exception.Response.StatusDescription)"
		Write-PSFMessage -Level Error -Message "URL attempted: $($SCLogin.URL)/Services/AuthenticationService.ashx/TryLogin"
		Write-PSFMessage -Level Error -Message "Username used: $($SCLogin.Username)"
		start-sleep (get-random -Minimum 10 -Maximum 100)
		continue
	}
	if (!$AuthResponse) {
		$attempt--
		Write-PSFMessage -Level Error -Message "Failed to connect to: ScreenConnect"
		start-sleep (get-random -Minimum 10 -Maximum 100)
		continue
	}

	# Download the full device list report and then import it
	$Response = Invoke-WebRequest "$($SCLogin.URL)/Report.csv?ReportType=Session&SelectFields=SessionID&SelectFields=Name&SelectFields=GuestMachineName&SelectFields=GuestMachineSerialNumber&SelectFields=GuestHardwareNetworkAddress&SelectFields=GuestOperatingSystemName&SelectFields=GuestLastActivityTime&SelectFields=GuestInfoUpdateTime&SelectFields=GuestLastBootTime&SelectFields=GuestLoggedOnUserName&SelectFields=GuestLoggedOnUserDomain&SelectFields=GuestMachineManufacturerName&SelectFields=GuestMachineModel&SelectFields=GuestMachineDescription&SelectFields=CustomProperty1&SelectFields=GuestSystemMemoryTotalMegabytes&SelectFields=GuestProcessorName&SelectFields=GuestProcessorVirtualCount&Filter=SessionType%20%3D%20'Access'%20AND%20NOT%20IsEnded&AggregateFilter=&ItemLimit=100000" -WebSession $SCWebSession
	$SC_Devices_Full = $Response.Content | ConvertFrom-Csv

	# If bad results
	if (($Response.Headers.Keys.Contains("P3P") -and $Response.Headers.P3P -like "*NON CUR OUR STP STA PRE*") -or 
		$Response.Headers.'Content-Type' -like "text/html;*" -or 
		$SC_Devices_Full[0].PSObject.Properties.Name.Count -le 1 -or 
		$SC_Devices_Full[0].PSObject.Properties.Name -contains "H1") 
	{
		$attempt--
		Write-PSFMessage -Level Error -Message "Failed to get: Device List from ScreenConnect"
		Write-PSFMessage -Level Error -Message "StatusCode: $($Response.StatusCode)"
		Write-PSFMessage -Level Error -Message "Message: $($_.Exception.Message)"
		Write-PSFMessage -Level Error -Message "StatusDescription: $($Response.StatusDescription)"

		Write-PSFMessage -Level Error -Message "Headers: $($Response.Headers | ConvertTo-Json)"
		Write-PSFMessage -Level Error -Message "Auth Content: $($AuthResponse.RawContent)"
		Write-PSFMessage -Level Error -Message "Auth Headers: $($AuthResponse.Headers)"
		Write-PSFMessage -Level Error -Message "BaseResponse: $($Response.BaseResponse | ConvertTo-Json)"

		start-sleep (get-random -Minimum 10 -Maximum 100)
		continue
	} else {
		# Success
		Write-PSFMessage -Level Verbose -Message "Successfully got $($SC_Devices_Full.Length) devices from ScreenConnect."
		break
	}
}

#################
# Functions (the 'if' just allows me to collapse them all)
#################
if ($true) {

	# Function to convert imported UTC date/times to local time for easier comparisons
	function Convert-UTCtoLocal {
		param( [parameter(Mandatory=$true)] [String] $UTCTime )
		$strCurrentTimeZone = (Get-WmiObject win32_timezone).StandardName 
		$TZ = [System.TimeZoneInfo]::FindSystemTimeZoneById($strCurrentTimeZone) 
		$LocalTime = [System.TimeZoneInfo]::ConvertTimeFromUtc($UTCTime, $TZ)
		return $LocalTime
	}

	# The id on each Sophos device is not the same ID that is used on the website
	# To get this ID, we must invert each pair of characters in the ID
	function convert_sophos_id_to_web($EndpointID) {
		$WebEndpointID = ""
		$Length = $EndpointID.length
		
		for ($i = 0; $i -lt $Length; $i ++) {
			if ($EndpointID[$i] -eq "-") {
				$WebEndpointID += "-"
				continue
			}
			$WebEndpointID += $EndpointID[$i+1]
			$WebEndpointID += $EndpointID[$i]
			$i++
		}

		return $WebEndpointID
	}

	# This function checks the force match array for any forced matches and returns them
	# Input the device ID and the $Type of connection (SC, RMM, Sophos, or ITG)
	# Returns an array containing hashtables for each match with the matching connections type, and device id, and if we want to match with this id or ignore this id
	# @( @{"type" = "sc, rmm, sophos, or itg", "id" = "device id or $false for no match", "match" = $true if we want to match to this ID, $false if we want to block matches with this id } )
	function force_match($DeviceID, $Type) {
		$Types = @("SC", "RMM", "Sophos", "ITG")
		if (!$Type -or $Type -notin $Types) {
			return
		}

		$ForcedMatches = @()
		foreach ($DefaultType in $Types) {
			# For each entry in the type we are getting, get all by from id
			if ($DefaultType -like $Type) {
				foreach ($Match in $ForceMatch.$Type) {
					# Check if id matches, if this is a sophos match, see if the inverted id matches as well
					if ($Match.from -like $DeviceID -or (($Match.tosystem -like "Sophos" -or $DefaultType -like "Sophos") -and (convert_sophos_id_to_web $Match.from) -like $DeviceID)) {
						$ForcedMatches += @{
							type = $Match.tosystem
							id = $Match.to
							match = [bool]$Match.to
						}
					}
				}
			# For each entry in other types, get all by to id
			} else {
				foreach ($Match in $ForceMatch.$DefaultType) {
					if ($Match.tosystem -like $Type -and ($Match.to -like $DeviceID -or $Match.to -eq $false -or (($Match.tosystem -like "Sophos" -or $DefaultType -like "Sophos") -and (convert_sophos_id_to_web $Match.to) -like $DeviceID))) {
						$ForcedMatches += @{
							type = $DefaultType
							id = $Match.from
							match = [bool]$Match.to
						}
					}
				}
			}
		}

		# Convert and add a new entry for any sophos ids into their web id equivalent as well (sophos uses inverted ids on their website which is most likely what will be entered in the $ForceMatches section)
		$ForcedMatchesCopy = $ForcedMatches
		foreach ($Match in $ForcedMatchesCopy) {
			if ($Match.type -like "Sophos" -and $Match.id) {
				$ForcedMatches += @{
					type = $Match.type
					id = convert_sophos_id_to_web $Match.id
					match = $Match.match
				}
			}
		}

		return $ForcedMatches
	}

	# Function for logging automated changes (installs, deletions, etc.)
	# $ServiceTarget is 'rmm', 'sc', 'sophos', 'jc', 'itg', or 'autotask'
	function log_change($Company_Acronym, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $JC_Device_ID = $false, $ChangeType, $Hostname = "", $Reason = "") {
		if (!$LogLocation) {
			return $false
		}
		if (!(Test-Path -Path $LogLocation)) {
			New-Item -ItemType Directory -Force -Path $LogLocation | Out-Null
		}
	
		$Now = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).ToUniversalTime()).TotalSeconds
		$LogFilePath = "$($LogLocation)\$($Company_Acronym)_log.json"
		if (Test-Path $LogFilePath) {
			$LogData = Get-Content -Path $LogFilePath -Raw | ConvertFrom-Json
			if ($LogData.GetType().BaseType -ne "System.Array") {
				$LogData = @($LogData)
			}
		} else {
			$LogData = @()
		}
	
		$LogData += [pscustomobject]@{
			rmm_id = $RMM_Device_ID
			sc_id = $SC_Device_ID
			sophos_id = $Sophos_Device_ID
			jc_id = $JC_Device_ID
			service_target = $ServiceTarget
			change = $ChangeType
			hostname = $Hostname
			reason = $Reason
			datetime = $Now
		}
	
		$LogData | ConvertTo-Json -Depth 5 | Out-File -FilePath $LogFilePath
	}

	# Function for querying a portion of the log history based on the possible filters
	# $LogHistory is the loaded json from the companies history log
	# StartTime is the unixtimestamp to start selection from (inclusive)
	# EndTime is the unixtimestamp to end selection at (exclusive) (if you set it to 'now' it will automatically calculate the current timestamp)
	# the remainder are optional and can be used as filters, hostname and reason support wildcards
	function log_query($LogHistory, $StartTime, $EndTime, $ServiceTarget = "", $RMM_Device_ID = "", $SC_Device_ID = "", $Sophos_Device_ID = "", $ChangeType = "", $Hostname = "", $Reason = "") {
		if ($EndTime -eq 'now') {
			$EndTime = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).ToUniversalTime()).TotalSeconds
		}

		$History_TimeSubset = $LogHistory | Where-Object { $_.datetime -ge $StartTime -and $_.datetime -lt $EndTime }
		
		$History_Filtered = $History_TimeSubset
		if ($ServiceTarget) {
			$History_Filtered = $History_Filtered | Where-Object { $_.service_target -eq $ServiceTarget }
		}
		if ($RMM_Device_ID) {
			$History_Filtered = $History_Filtered | Where-Object { $RMM_Device_ID -in $_.rmm_id }
		}
		if ($SC_Device_ID) {
			$History_Filtered = $History_Filtered | Where-Object { $SC_Device_ID -in $_.sc_id }
		}
		if ($Sophos_Device_ID) {
			$History_Filtered = $History_Filtered | Where-Object { $Sophos_Device_ID -in $_.sophos_id }
		}
		if ($ChangeType) {
			$History_Filtered = $History_Filtered | Where-Object { $_.change -eq $ChangeType }
		}
		if ($Hostname) {
			$History_Filtered = $History_Filtered | Where-Object { $_.hostname -like $Hostname }
		}
		if ($Reason) {
			$History_Filtered = $History_Filtered | Where-Object { $_.reason -like $Reason }
		}
		
		return $History_Filtered;
	}

	# Helper function that queries the log based on the filters and returns the count of entries found
	function log_attempt_count($LogHistory, $ServiceTarget = "", $RMM_Device_ID = "", $SC_Device_ID = "", $Sophos_Device_ID = "", $ChangeType = "", $Hostname = "", $Reason = "") {
		$History = log_query -LogHistory $LogHistory -StartTime 0 -EndTime 'now' -ServiceTarget $ServiceTarget -RMM_Device_ID $RMM_Device_ID -SC_Device_ID $SC_Device_ID -Sophos_Device_ID $Sophos_Device_ID -ChangeType $ChangeType -Hostname $Hostname -Reason $Reason
		return ($History | Measure-Object).Count
	}

	# This function finds the difference in seconds between the oldest and newest unixtimestamp in a set of log history
	function log_time_diff($LogHistory) {
		$Newest = $LogHistory | Sort-Object -Property datetime -Descending | Select-Object -First 1
		$Oldest = $LogHistory | Sort-Object -Property datetime | Select-Object -First 1
		return $Newest.datetime - $Oldest.datetime
	}

	# Function for querying repair tickets based on the possible filters
	# $ServiceTarget is 'rmm', 'sc', or 'sophos'
	# $Hostname can be a single hostname or an array of hostnames to check for
	function repair_tickets($ServiceTarget = "", $Hostname = "") {
		if ($ServiceTarget -eq 'rmm') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "RMM *" -or $_.title -like "* RMM" -or $_.title -like "* RMM *" }
		} elseif ($ServiceTarget -eq 'sc') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "SC *" -or $_.title -like "* SC" -or $_.title -like "* SC *" -or $_.title -like "*ScreenConnect*" }
		} elseif ($ServiceTarget -eq 'sophos') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "Sophos *" -or $_.title -like "* Sophos" -or $_.title -like "* Sophos *" }
		} else {
			$RepairTickets_ByService = $RepairTickets
		}

		if ($Hostname -is [array]) {
			$RepairTickets_FilteredIDs = @()
			foreach ($UniqueHostname in $Hostname) {
				$RepairTickets_FilteredIDs += ($RepairTickets_ByService | Where-Object { $_.title -like "*$($UniqueHostname)*" }).Id
			}
			$RepairTickets_FilteredIDs = $RepairTickets_FilteredIDs | Sort-Object -Unique
			$RepairTickets_Filtered = $RepairTickets_ByService | Where-Object { $_.Id -in $RepairTickets_FilteredIDs }
		} else {
			$RepairTickets_Filtered = $RepairTickets_ByService | Where-Object { $_.title -like "*$($Hostname)*" }
		}

		return $RepairTickets_Filtered;
	}

	# Checks the log history to see if something has been attempted more than 5 times
	# and attempts have been made for the past 2 weeks
	# If so, an email is sent if one hasn't already been sent in the last 2 weeks, and the email is logged
	# $ErrorMessage can use HTML and it will become the main body of the email sent
	function check_failed_attempts($LogHistory, $Company_Acronym, $ErrorMessage, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $ChangeType, $Hostname = "", $Reason = "") {
		$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
		$TenDays = [int](New-TimeSpan -Start (Get-Date).AddDays(-10).ToUniversalTime() -End (Get-Date).ToUniversalTime()).TotalSeconds

		# first check if a repair ticket already exists for this device/service
		if ($ServiceTarget -in ("rmm", "sc", "sophos")) {
			$RepairTickets_Filtered = repair_tickets -ServiceTarget $ServiceTarget -Hostname $Hostname
			if (($RepairTickets_Filtered | Measure-Object).count -gt 0) {
				break;
			}
		}

		# next check if an email was sent about this in the last 2 weeks
		if ($ChangeType) {
			$EmailChangeType = $ChangeType + "-email"
		}
		if ($Reason) {
			$EmailReason = "Email sent for " + $Reason
		} else {
			$EmailReason = "Email sent for $ChangeType"
		}

		$ID_Params = @{}
		$EmailLink = ""
		if ($ServiceTarget -eq 'rmm') {
			$ID_Params."RMM_Device_ID" = $RMM_Device_ID
			$RMMDevice = $RMM_DevicesHash[$RMM_Device_ID]
			if ($RMMDevice.url) {
				$EmailLink = $RMMDevice.url
			} else {
				$EmailLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMM_Device_ID)"
			}
		} elseif ($ServiceTarget -eq 'sc') {
			$ID_Params."SC_Device_ID" = $SC_Device_ID
			$SCDevice = $SC_DevicesHash[$SC_Device_ID]
			$EmailLink = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevice.Name)/$SC_Device_ID"
		} elseif ($ServiceTarget -eq 'sophos') {
			$ID_Params."Sophos_Device_ID" = $Sophos_Device_ID
			$EmailLink = "https://cloud.sophos.com/manage/devices/computers/$($Sophos_Device_ID)"
		}

		$EmailQuery_Params = @{
			LogHistory = $LogHistory
			StartTime = $TwoWeeksAgo
			EndTime = 'now'
			ServiceTarget = $ServiceTarget
			ChangeType = $EmailChangeType
			Hostname = $Hostname
			Reason = $EmailReason
		} + $ID_Params
		$History_Subset_Emails = log_query @EmailQuery_Params

		# no emails were already sent, continue
		if (($History_Subset_Emails | Measure-Object).count -eq 0) {
			$FilterQuery_Params = @{
				LogHistory = $LogHistory
				StartTime = $TwoWeeksAgo
				EndTime = 'now'
				ServiceTarget = $ServiceTarget
				ChangeType = $ChangeType
				Hostname = $Hostname
				Reason = $Reason
			} + $ID_Params
			$History_Filtered = log_query @FilterQuery_Params

			# if attempts have been made over at least a 10 day span AND a minimum of 5 attempts have been made
			if (($History_Filtered | Measure-Object).count -ge 5 -and (log_time_diff($History_Filtered)) -ge $TenDays) {
				# send an email
				$EmailSubject = "Device Audit - Auto-Fix Failed: $Hostname ($Company_Acronym)"
				$EmailIntro = "An auto-fix has been attempted on $Hostname more than 5 times in the past 2 weeks yet the issue is still not resolved. Please resolve this manually."

				$HTMLEmail = $EmailTemplate -f `
								$EmailIntro, 
								"Auto-Fix has repeatedly failed", 
								$ErrorMessage, 
								"<br />Link: <a href='$EmailLink'>$EmailLink</a> <br /><br />The audit will continue to attempt to automatically fix this issue, but it will likely keep failing. Please resolve this manually."

				$mailbody = @{
					"From" = $EmailFrom
					"To" = $EmailTo_FailedFixes
					"Subject" = $EmailSubject
					"HTMLContent" = $HTMLEmail
				} | ConvertTo-Json -Depth 6

				$headers = @{
					'x-api-key' = $Email_APIKey.Key
				}

				Invoke-RestMethod -Method Post -Uri $Email_APIKey.Url -Body $mailbody -Headers $headers -ContentType application/json
				Write-Host "Multiple Failed Auto-Fix Attempts Found. Email Sent." -ForegroundColor Yellow

				# log the sent email
				log_change -Company_Acronym $Company_Acronym -ServiceTarget $ServiceTarget -RMM_Device_ID $RMM_Device_ID -SC_Device_ID $SC_Device_ID -Sophos_Device_ID $Sophos_Device_ID -ChangeType $EmailChangeType -Hostname $Hostname -Reason $EmailReason
			}
		}
	}

	# Functions for comparing devices by their last active date
	# Pass the function a list of device ids
	# Each returns an ordered list with the type (rmm, sc, or sophos), device ID and last active date, ordered with newest first, $null if no date
	$UnixDateLowLimit = Get-Date -Year 1970 -Month 1 -Day 1 -Hour 0 -Minute 0 -Second 0
	function compare_activity_sc($DeviceIDs) {
		if (!$SC_DevicesHash) {
			return @()
		}

		$SCDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$SCDevices += $SC_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $SCDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "sc"
				id = $Device.SessionID
				last_active = $null
			}

			if ($Device.GuestLastSeen -and [string]$Device.GuestLastSeen -as [DateTime] -and [DateTime]$Device.GuestLastSeen -gt $UnixDateLowLimit) {
				$DeviceOutputObj.last_active = [DateTime]$Device.GuestLastSeen
				$DevicesOutput += $DeviceOutputObj
			} elseif ($Device.GuestLastActivityTime -and [string]$Device.GuestLastActivityTime -as [DateTime] -and [DateTime]$Device.GuestLastActivityTime -gt $UnixDateLowLimit) {
				$DeviceOutputObj.last_active = [DateTime]$Device.GuestLastActivityTime
				$DevicesOutput += $DeviceOutputObj
			} elseif ($Device.GuestInfoUpdateTime -and [string]$Device.GuestInfoUpdateTime -as [DateTime] -and [DateTime]$Device.GuestInfoUpdateTime -gt $UnixDateLowLimit) {
				$DeviceOutputObj.last_active = [DateTime]$Device.GuestInfoUpdateTime
				$DevicesOutput += $DeviceOutputObj
			} elseif ($Device.GuestLastBootTime -and [string]$Device.GuestLastBootTime -as [DateTime] -and [DateTime]$Device.GuestLastBootTime -gt $UnixDateLowLimit) {
				$DeviceOutputObj.last_active = [DateTime]$Device.GuestLastBootTime
				$DevicesOutput += $DeviceOutputObj
			}
		}
		
		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	function compare_activity_rmm($DeviceIDs) {
		if (!$RMM_DevicesHash) {
			return @()
		}

		$Now = Get-Date
		$RMMDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$RMMDevices += $RMM_DevicesHash[$DeviceID]
		}

		$DevicesOutput = @()

		foreach ($Device in $RMMDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "rmm"
				id = $Device."Device UID"
				last_active = $null
			}

			if ($Device.Status -eq "Online" -or $Device."Last Seen" -eq "Currently Online") {
				$DeviceOutputObj.last_active = $Now
				$DevicesOutput += $DeviceOutputObj
			} elseif ($Device."Last Seen" -and [string]$Device."Last Seen" -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device."Last Seen"
				$DevicesOutput += $DeviceOutputObj
			}
		}

		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	function compare_activity_sophos($DeviceIDs) {
		if (!$Sophos_DevicesHash) {
			return @()
		}

		$SophosDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$SophosDevices += $Sophos_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $SophosDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "sophos"
				id = $Device.id
				last_active = $null
			}

			if ($Device.lastSeenAt -and [string]$Device.lastSeenAt -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device.lastSeenAt
				$DevicesOutput += $DeviceOutputObj
			}
		}

		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	function compare_activity_jc($DeviceIDs) {
		if (!$JC_DevicesHash) {
			return @()
		}

		$JCDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$JCDevices += $JC_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $JCDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "jc"
				id = $Device.id
				last_active = $null
			}

			if ($Device.lastContact -and [string]$Device.lastContact -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device.lastContact
				$DevicesOutput += $DeviceOutputObj
			}
		}

		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	function compare_activity_azure($DeviceIDs) {
		if (!$Azure_DevicesHash) {
			return @()
		}

		$AzureDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$AzureDevices += $Azure_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $AzureDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "azure"
				id = $Device.id
				last_active = $null
			}

			if ($Device.ApproximateLastSignInDateTime -and [string]$Device.ApproximateLastSignInDateTime -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device.ApproximateLastSignInDateTime
				$DevicesOutput += $DeviceOutputObj
			}
		}

		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	function compare_activity_intune($DeviceIDs) {
		if (!$Intune_DevicesHash) {
			return @()
		}

		$IntuneDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$IntuneDevices += $Intune_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $IntuneDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "intune"
				id = $Device.id
				last_active = $null
			}

			if ($Device.LastSyncDateTime -and [string]$Device.LastSyncDateTime -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device.LastSyncDateTime
				$DevicesOutput += $DeviceOutputObj
			}
		}

		$DevicesOutput = $DevicesOutput | Sort-Object last_active -Desc

		$DevicesOutput
		return
	}

	# Helper function that takes a $MatchedDevices object and returns the activity comparison for SC, RMM, Sophos, and (if applicable) Azure & Intune
	function compare_activity($MatchedDevice) {
		$Activity = @{}

		if ($MatchedDevice.sc_matches -and $MatchedDevice.sc_matches.count -gt 0) {
			$SCActivity = compare_activity_sc $MatchedDevice.sc_matches
			$Activity.sc = $SCActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		if ($MatchedDevice.rmm_matches -and $MatchedDevice.rmm_matches.count -gt 0) {
			$RMMActivity = compare_activity_rmm $MatchedDevice.rmm_matches
			$Activity.rmm = $RMMActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		if ($MatchedDevice.sophos_matches -and $MatchedDevice.sophos_matches.count -gt 0) {
			$SophosActivity = compare_activity_sophos $MatchedDevice.sophos_matches
			$Activity.sophos = $SophosActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		if ($JCConnected -and $MatchedDevice.jc_matches -and $MatchedDevice.jc_matches.count -gt 0) {
			$JCActivity = compare_activity_jc $MatchedDevice.jc_matches
			$Activity.jc = $JCActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		if ($MatchedDevice.azure_matches -and $MatchedDevice.azure_matches.count -gt 0) {
			$AzureActivity = compare_activity_azure $MatchedDevice.azure_matches
			$Activity.azure = $AzureActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		if ($MatchedDevice.intune_matches -and $MatchedDevice.intune_matches.count -gt 0) {
			$IntuneActivity = compare_activity_intune $MatchedDevice.intune_matches
			$Activity.intune = $IntuneActivity | Sort-Object last_active -Descending | Select-Object -First 1
		}

		$Activity
		return
	}

	# Adds a device to the install queue json file (creates the file if need be)
	# Either set $SC_ID or $RMM_ID, this is the device used for the install
	# Set $ToInstall to what needs to be installed ("sc", or "rmm")
	function add_device_to_install_queue($SC_ID = $false, $RMM_ID = $false, $ToInstall = $false) {
		$FromDeviceType = $false
		if ($SC_ID) {
			$FromDeviceType = "sc"
		} elseif ($RMM_ID) {
			$FromDeviceType = "rmm"
		} else {
			return $false
		}

		if (!$ToInstall) { return $false }

		$InstallQueue = [PSCustomObject]@{}
		if (Test-Path $InstallQueuePath) {
			$InstallQueue = Get-Content -Path $InstallQueuePath -Raw | ConvertFrom-Json
			if (!$InstallQueue) {
				$InstallQueue = [PSCustomObject]@{}
			}
		}
		
		if (!$InstallQueue.PSObject.Properties -or $InstallQueue.PSObject.Properties.Name -notcontains $ToInstall) {
			$InstallQueue | Add-Member -NotePropertyName $ToInstall -NotePropertyValue $false
			$InstallQueue.($ToInstall) = [PSCustomObject]@{}
		}

		if (!$InstallQueue.($ToInstall).PSObject.Properties -or $InstallQueue.($ToInstall).PSObject.Properties.Name -notcontains $FromDeviceType) {
			$InstallQueue.($ToInstall) | Add-Member -NotePropertyName $FromDeviceType -NotePropertyValue @()
		}

		if ($FromDeviceType -eq "sc") {
			if ($SC_ID -notin $InstallQueue.($ToInstall).($FromDeviceType)) {
				$InstallQueue.($ToInstall).($FromDeviceType) += $SC_ID
			}
		} else {
			if ($RMM_ID -notin $InstallQueue.($ToInstall).($FromDeviceType)) {
				$InstallQueue.($ToInstall).($FromDeviceType) += $RMM_ID
			}
		}

		if ($InstallQueue -and $InstallQueuePath) {
			$InstallQueue | ConvertTo-Json -Depth 5 | Out-File -FilePath $InstallQueuePath
		}

		return $true
	}

	# Removes a device from the install queue json file
	# Either set $SC_ID or $RMM_ID, this is the device used for the install
	# Set $ToInstall to what needed to be installed ("sc" or "rmm"), if left $false will remove for all $ToInstall types
	function remove_device_from_install_queue($SC_ID = $false, $RMM_ID = $false, $ToInstall = $false) {
		$FromDeviceType = $false
		if ($SC_ID) {
			$FromDeviceType = "sc"
		} elseif ($RMM_ID) {
			$FromDeviceType = "rmm"
		} else {
			return $false
		}

		$InstallQueue = [PSCustomObject]@{}
		if (Test-Path $InstallQueuePath) {
			$InstallQueue = Get-Content -Path $InstallQueuePath -Raw | ConvertFrom-Json
			if (!$InstallQueue) {
				$InstallQueue = [PSCustomObject]@{}
			}
		}

		if ($ToInstall -and $InstallQueue.PSObject.Properties -and $InstallQueue.PSObject.Properties.Name -contains $ToInstall -and $InstallQueue.($ToInstall).PSObject.Properties -and $InstallQueue.($ToInstall).PSObject.Properties.Name -contains $FromDeviceType) {
			if ($FromDeviceType -eq "sc") {
				$InstallQueue.($ToInstall).($FromDeviceType) = @($InstallQueue.($ToInstall).($FromDeviceType) | Where-Object { $_ -notlike $SC_ID })
			} else {
				$InstallQueue.($ToInstall).($FromDeviceType) = @($InstallQueue.($ToInstall).($FromDeviceType) | Where-Object { $_ -notlike $RMM_ID })
			}
		} elseif (!$ToInstall -and $InstallQueue.PSObject.Properties) {
			foreach ($InstallType in $InstallQueue.PSObject.Properties.Name) {
				if ($InstallQueue.($InstallType).PSObject.Properties -and $InstallQueue.($InstallType).PSObject.Properties.Name -contains $FromDeviceType) {
					if ($FromDeviceType -eq "sc") {
						$InstallQueue.($InstallType).($FromDeviceType) = @($InstallQueue.($InstallType).($FromDeviceType) | Where-Object { $_ -notlike $SC_ID })
					} else {
						$InstallQueue.($InstallType).($FromDeviceType) = @($InstallQueue.($InstallType).($FromDeviceType) | Where-Object { $_ -notlike $RMM_ID })
					}
				}
			}
		}

		if ($InstallQueue -and $InstallQueuePath) {
			$InstallQueue | ConvertTo-Json -Depth 5 | Out-File -FilePath $InstallQueuePath
		}

		return $true
	}

	# Helper function that checks a device from the $MatchedDevices array against the $Ignore_Installs config value and returns true if it should be ignored
	# Also ignores Sophos on Hyper-V hosts via a regex check
	# $System should be 'sc', 'rmm', or 'sophos'
	function ignore_install($Device, $System) {
		if ($System -eq 'sc' -and $Ignore_Installs.SC -eq $true) {
			return $true
		} elseif ($System -eq 'rmm' -and $Ignore_Installs.RMM -eq $true) {
			return $true
		} elseif ($System -eq 'sophos' -and $Ignore_Installs.Sophos -eq $true) {
			return $true
		}

		if ($System -eq 'sophos' -and ($Device.rmm_hostname -match $HyperVRegex -or $Device.sc_hostname -match $HyperVRegex)) {
			return $true;
		}

		$IgnoredDevices = @()
		if ($System -eq 'sc') {
			$IgnoredDevices = $Ignore_Installs.SC
		} elseif ($System -eq 'rmm') {
			$IgnoredDevices = $Ignore_Installs.RMM
		} elseif ($System -eq 'sophos') {
			$IgnoredDevices = $Ignore_Installs.Sophos
		}

		if ($IgnoredDevices) {
			if ($System -eq 'sc' -and ($IgnoredDevices | Where-Object { $_ -in $Device.rmm_hostname -or $_ -in $Device.rmm_matches -or $_ -in $Device.sophos_hostname -or $_ -in $Device.sophos_matches } | Measure-Object).Count -gt 0) {
				return $true
			} elseif ($System -eq 'rmm' -and ($IgnoredDevices | Where-Object { $_ -in $Device.sc_hostname -or $_ -in $Device.sc_matches -or $_ -in $Device.sophos_hostname -or $_ -in $Device.sophos_matches } | Measure-Object).Count -gt 0) {
				return $true
			} elseif ($System -eq 'sophos' -and ($IgnoredDevices | Where-Object { $_ -in $Device.sc_hostname -or $_ -in $Device.sc_matches -or $_ -in $Device.rmm_hostname -or $_ -in $Device.rmm_matches  } | Measure-Object).Count -gt 0) {
				return $true
			}
		} else {
			return $false
		}
	}

	# Deletes a device from ScreenConnect
	function delete_from_sc($SC_ID, $SCWebSession) {
		# Get an anti-forgery token from the website
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Host#Access/All%20Machines//$SC_ID" -WebSession $SCWebSession -Method 'POST' -ContentType 'application/json'
		$Response.RawContent -match '"antiForgeryToken":"(.+?)"' | Out-Null
		$AntiForgeryToken = $matches[1]

		if ($AntiForgeryToken) {
			$FormBody = '[["All Machines"],[{"SessionID": "' + $SC_ID + '","EventType":21,"Data":null}]]'
			$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddSessionEvents" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'
			$Removed = remove_device_from_install_queue -SC_ID $SC_ID -ToInstall $false
			return $true
		} else {
			Write-Warning "Could not get an anti-forgery token from Screenconnect. Failed to delete device from SC: $SC_ID"
			return $false
		}
	}

	# This doesn't truly delete a device from RMM (we can't using the API), instead it sets the Delete Me UDF which adds the device into the device filter of devices we should delete
	function delete_from_rmm($RMM_Device_ID) {
		Set-DrmmDeviceUdf -deviceUid $RMM_Device_ID -udf30 "True"
		$Removed = remove_device_from_install_queue -RMM_ID $RMM_Device_ID -ToInstall $false
	}

	# Deletes a device from Sophos (be careful with this, make sure it no longer is installed on the device!)
	# Only works if using the sophos API. Pass in the same headers used for getting the endpoints.
	function delete_from_sophos($Sophos_Device_ID, $TenantApiHost, $SophosHeader) {
		if ($Sophos_Device_ID -and $TenantApiHost -and $SophosHeader) {
			try {
				$Response = Invoke-RestMethod -Method DELETE -Headers $SophosHeader -uri ($TenantApiHost + "/endpoint/v1/endpoints/$Sophos_Device_ID")
				if ($Response.deleted -eq "True") {
					return $true
				}
			} catch {
				Write-PSFMessage -Level Error -Message "Could not auto-delete Sophos device '$Sophos_Device_ID' for the reason: " + $_.Exception.Message
			}
		}
		return $false
	}

	# Deletes a device from JumpCloud
	function delete_from_jc($JC_ID) {
		$Deleted = $false
		if ($JC_ID) {
			$Deleted = Remove-JCSystem -SystemID $JC_ID -Force
		}

		if ($Deleted -and $Deleted.Results -like "Deleted") {
			return $true
		} else {
			Write-Warning "Could not delete device '$($JC_ID)' from JumpCloud."
			return $false
		}
	}

	# Archives a configuration in ITG
	function archive_itg($ITG_Device_ID) {
		$UpdatedConfig = @{
			'type' = 'configurations'
			'attributes' = @{
				'archived' = 'true'
			}
		}

		try {
			Set-ITGlueConfigurations -id $ITG_Device_ID -data $UpdatedConfig
			return $true
		} catch {
			Write-PSFMessage -Level Error -Message "Could not archive ITG configuration '$ITG_Device_ID' for the reason: " + $_.Exception.Message
			return $false
		}
	}

	# Cleans up the name of a Manufacturer
	function manufacturer_cleanup($Manufacturer) {
		if ($Manufacturer) {
			$CleanedManufacturer = $Manufacturer
			if ($CleanedManufacturer -like "*/*") {
				$CleanedManufacturer = ($CleanedManufacturer -split '/')[0]
			}
			$CleanedManufacturer = $CleanedManufacturer.Trim()
			$CleanedManufacturer = $CleanedManufacturer -replace ",? ?(Inc\.?$|Corporation$|Corp\.?$|Co\.$|Ltd\.?$)", ""
			$CleanedManufacturer = $CleanedManufacturer.Trim()
			$CleanedManufacturer = $CleanedManufacturer -replace ",? ?(Inc\.?$|Corporation$|Corp\.?$|Co\.$|Ltd\.?$)", ""
			$CleanedManufacturer = $CleanedManufacturer.Trim()

			return $CleanedManufacturer
		} else {
			return $null
		}
	}

	# Converts a subnet mask into a Cidr range
	function Convert-SubnetMaskToCidr($Subnet) {
		# From: https://codeandkeep.com/PowerShell-Get-Subnet-NetworkID/
		$NetMaskIP = [IPAddress]$Subnet
		$BinaryString = [String]::Empty
		$NetMaskIP.GetAddressBytes() | ForEach-Object {
			$BinaryString += [Convert]::ToString($_, 2)
		}
		return $binaryString.TrimEnd('0').Length
	}

	# Levenshtein distance function for comparing similarity between two strings
	function Measure-StringDistance {
		<#
			.SYNOPSIS
				Compute the distance between two strings using the Levenshtein distance formula.
			
			.DESCRIPTION
				Compute the distance between two strings using the Levenshtein distance formula.

			.PARAMETER Source
				The source string.

			.PARAMETER Compare
				The comparison string.

			.EXAMPLE
				PS C:\> Measure-StringDistance -Source "Michael" -Compare "Micheal"

				2

				There are two characters that are different, "a" and "e".

			.EXAMPLE
				PS C:\> Measure-StringDistance -Source "Michael" -Compare "Michal"

				1

				There is one character that is different, "e".

			.NOTES
				Author:
				Michael West
		#>

		[CmdletBinding(SupportsShouldProcess=$true)]
		[OutputType([int])]
		param (
			[Parameter(ValueFromPipelineByPropertyName=$true)]
			[string]$Source = "",
			[string]$Compare = ""
		)
		$n = $Source.Length;
		$m = $Compare.Length;
		$d = New-Object 'int[,]' $($n+1),$($m+1)
			
		if ($n -eq 0){
		return $m
		}
		if ($m -eq 0){
			return $n
		}

		for ([int]$i = 0; $i -le $n; $i++){
			$d[$i, 0] = $i
		}
		for ([int]$j = 0; $j -le $m; $j++){
			$d[0, $j] = $j
		}

		for ([int]$i = 1; $i -le $n; $i++){
			for ([int]$j = 1; $j -le $m; $j++){
				if ($Compare[$($j - 1)] -eq $Source[$($i - 1)]){
					$cost = 0
				}
				else{
					$cost = 1
				}
				$d[$i, $j] = [Math]::Min([Math]::Min($($d[$($i-1), $j] + 1), $($d[$i, $($j-1)] + 1)),$($d[$($i-1), $($j-1)]+$cost))
			}
		}
			
		return $d[$n, $m]
	}

	# Functions for installing/reinstalling SC/RMM/Sophos

	# Installs RMM on a device using ScreenConnect
	# $SC_ID the GUID of the device in screenconnect
	# $RMM_ORG_ID the GUID of the organization in RMM (found in the organization under Settings > General > ID)
	# $SCWebSession the web session used to authenticate with Screenconnect previously
	function install_rmm_using_sc($SC_ID, $RMM_ORG_ID, $SCWebSession) {
		# Get an anti-forgery token from the website
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Host#Access/All%20Machines//$SC_ID" -WebSession $SCWebSession -Method 'POST' -ContentType 'application/json'
		$Response.RawContent -match '"antiForgeryToken":"(.+?)"' | Out-Null
		$AntiForgeryToken = $matches[1]

		if ($AntiForgeryToken) {
			$RMMInstallCmd = "#timeout=100000\npowershell -command \`"[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12; Invoke-WebRequest -Uri 'https://$($DattoAPIKey.Region).centrastage.net/csm/profile/downloadAgent/$RMM_ORG_ID' -OutFile c:\\windows\\temp\\AEM-Installer.exe; c:\\windows\\temp\\AEM-Installer.exe /s; Write-Host 'RMM Install Complete';\`""

			$FormBody = '[["All Machines"],[{"SessionID": "' + $SC_ID + '","EventType":44,"Data":"' + $RMMInstallCmd + '"}]]'
			$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddSessionEvents" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'
			$Removed = remove_device_from_install_queue -SC_ID $SC_ID -ToInstall "rmm"
			return $true
		} else {
			Write-Warning "Could not get an anti-forgery token from Screenconnect. Failed to install RMM for SC device ID: $SC_ID"
			return $false
		}
	}

	function install_rmm_using_sc_mac($SC_ID, $RMM_ORG_ID, $SCWebSession) {
		# Get an anti-forgery token from the website
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Host#Access/All%20Machines//$SC_ID" -WebSession $SCWebSession -Method 'POST' -ContentType 'application/json'
		$Response.RawContent -match '"antiForgeryToken":"(.+?)"' | Out-Null
		$AntiForgeryToken = $matches[1]
	
		if ($AntiForgeryToken) {
			$RMMInstallCmd = "#timeout=100000\n#!bash\ncd /tmp\ncurl -o aem-installer.zip 'https://$($DattoAPIKey.Region).centrastage.net/csm/profile/downloadMacAgent/$RMM_ORG_ID'\nunzip -a aem-installer.zip\ncd AgentSetup\nsudo installer -pkg CAG.pkg -target /"
	
			$FormBody = '[["All Machines"],[{"SessionID": "' + $SC_ID + '","EventType":44,"Data":"' + $RMMInstallCmd + '"}]]'
			$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddSessionEvents" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'
			$Removed = remove_device_from_install_queue -SC_ID $SC_ID -ToInstall "rmm"
	
			return $true
		} else {
			Write-Warning "Could not get an anti-forgery token from Screenconnect. Failed to install RMM for SC device ID: $SC_ID"
			return $false
		}
	}

	function is_sc_installed($RMM_Device) {
		$DeviceSoftware = Get-DrmmAuditDeviceSoftware -DeviceUid $RMM_Device."Device UID"
		if (($DeviceSoftware | Where-Object { $_.name -like "ScreenConnect Client*" } | Measure-Object).Count -gt 0) {
			return $true
		} else {
			return $false
		}
	}

	function log_recent_rmm_job($RMM_Device, $JobType, $JobID) {
		$RecentRMMJobs = [PSCustomObject]@{}

		if (Test-Path $RecentRMMJobsPath) {
			$RecentRMMJobs = Get-Content -Path $RecentRMMJobsPath -Raw | ConvertFrom-Json
			if (!$RecentRMMJobs) {
				$RecentRMMJobs = [PSCustomObject]@{}
			}
		}
		
		if (!$RecentRMMJobs.PSObject.Properties -or $RecentRMMJobs.PSObject.Properties.Name -notcontains $RMM_Device."Device UID") {
			$RecentRMMJobs | Add-Member -NotePropertyName $RMM_Device."Device UID" -NotePropertyValue $false
			$RecentRMMJobs.($RMM_Device."Device UID") = [PSCustomObject]@{}
		}

		if (!$RecentRMMJobs.($RMM_Device."Device UID").PSObject.Properties -or $RecentRMMJobs.($RMM_Device."Device UID").PSObject.Properties.Name -notcontains $JobType) {
			$RecentRMMJobs.($RMM_Device."Device UID") | Add-Member -NotePropertyName $JobType -NotePropertyValue $false	
		}

		$RecentRMMJobs.($RMM_Device."Device UID").($JobType) = $JobID

		if ($RecentRMMJobs -and $RecentRMMJobsPath) {
			$RecentRMMJobs | ConvertTo-Json -Depth 5 | Out-File -FilePath $RecentRMMJobsPath
		}
	}

	function is_existing_rmm_job_active($RMM_Device, $JobType) {
		$RecentRMMJobs = $false
		if (Test-Path -Path $RecentRMMJobsPath) {
			$RecentRMMJobs = Get-Content -Path $RecentRMMJobsPath -Raw | ConvertFrom-Json
		}

		if ($RecentRMMJobs -and $RecentRMMJobs.($RMM_Device."Device UID") -and $RecentRMMJobs.($RMM_Device."Device UID").($JobType)) {
			$JobID = $RecentRMMJobs.($RMM_Device."Device UID").($JobType)
			$JobStatus = Get-DrmmJobStatus -jobUid $JobID

			if ($JobStatus -and $JobStatus.status -and $JobStatus.status -eq "active") {
				return $true
			}
		}

		return $false
	}

	function uninstall_sc_using_rmm($RMM_Device) {
		if ($RMM_Device."Operating System" -like "*Windows*" -and (is_sc_installed -RMM_Device $RMM_Device)) {
			if (is_existing_rmm_job_active -RMM_Device $RMM_Device -JobType "uninstall_sc") {
				return $false
			}
			$Job = Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Uninstall ScreenConnect on $($RMM_Device."Device Hostname")" -ComponentName "ConnectWise Control (ScreenConnect) Uninstaller [WIN]"
			if ($Job -and $Job.job -and $Job.job.uid) {
				log_recent_rmm_job -RMM_Device $RMM_Device -JobType "uninstall_sc" -JobID $Job.job.uid
				return $true
			}
		}
		return $false
	}

	function install_sc_using_rmm($RMM_Device) {
		if (is_existing_rmm_job_active -RMM_Device $RMM_Device -JobType "install_sc") {
			return $false
		}

		if ($RMM_Device."Operating System" -like "*Windows*") {
			$Job = Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Install ScreenConnect on $($RMM_Device."Device Hostname")" -ComponentName "ScreenConnect Install - WIN"
			if ($Job -and $Job.job -and $Job.job.uid) {
				$Removed = remove_device_from_install_queue -RMM_ID $RMM_Device."Device UID" -ToInstall "sc"
				log_recent_rmm_job -RMM_Device $RMM_Device -JobType "install_sc" -JobID $Job.job.uid
				return $true
			}
		} elseif ($RMM_Device."Operating System" -like "*Mac OS*") {
			$Job = Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Install ScreenConnect on $($RMM_Device."Device Hostname")" -ComponentName "ScreenConnect Install - MAC"
			if ($Job -and $Job.job -and $Job.job.uid) {
				$Removed = remove_device_from_install_queue -RMM_ID $RMM_Device."Device UID" -ToInstall "sc"
				log_recent_rmm_job -RMM_Device $RMM_Device -JobType "install_sc" -JobID $Job.job.uid
				return $true
			}
		}
		return $false
	}
}

### This code is unique for each company, lets loop through each company and run this code on each
foreach ($ConfigFile in $CompaniesToAudit) {
	. "$PSScriptRoot\Config Files\Global-Config.ps1" # Reimport Global Config to reset anything that was overridden
	. "$PSScriptRoot\Config Files\$ConfigFile" # Import company config
	Write-Output "============================="
	Write-Output "Starting cleanup for $Company_Acronym" 
	Write-PSFMessage -Level Verbose -Message "Starting cleanup on: $Company_Acronym"

	# Connect to JumpCloud (if applicable)
	$JCConnected = $false
	if ($JumpCloudAPIKey -and $JumpCloudAPIKey.Key) {
		try {
			Connect-JCOnline -JumpCloudApiKey $JumpCloudAPIKey.Key -Force -ErrorAction Stop
			$JCConnected = $true
		} catch {
			$JCConnected = $false
		}
	}

	# Connect to Microsoft Graph (for Azure/Intune)
	$AzureConnected = $false
	if ($AzureAppCredentials -and $Azure_TenantID) {
		$AuthBody = @{
			grant_type		= "client_credentials"
			scope			= "https://graph.microsoft.com/.default"
			client_id		= $AzureAppCredentials.AppID
			client_secret	= $AzureAppCredentials.ClientSecret
		}

		$conn = Invoke-RestMethod `
			-Uri "https://login.microsoftonline.com/$Azure_TenantID/oauth2/v2.0/token" `
			-Method POST `
			-Body $AuthBody

		$AzureToken = ConvertTo-SecureString -String $conn.access_token -AsPlainText -Force
		$MgGraphConnect = Connect-MgGraph -AccessToken $AzureToken
		if ($MgGraphConnect -eq "Welcome To Microsoft Graph!") {
			$AzureConnected = $true
		}
	}

	############
	# Connect to the Sophos API to get the device list from Sophos
	############

	# Refresh token if it has expired
	if ($SophosToken.expiry -lt (Get-Date)) {
		$SophosToken = Invoke-RestMethod -Method POST -Body $SophosGetTokenBody -ContentType "application/x-www-form-urlencoded" -uri "https://id.sophos.com/api/v2/oauth2/token"
		$SophosJWT = $SophosToken.access_token
		$SophosToken | Add-Member -NotePropertyName expiry -NotePropertyValue $null
		$SophosToken.expiry = (Get-Date).AddSeconds($SophosToken.expires_in)
	}

	# Get the tenants ID and URL
	if ($SophosTenants  -and $SophosTenants.items -and $Sophos_Company) {
		$CompanyInfo = $SophosTenants.items | Where-Object { $_.name -like $Sophos_Company }
		$SophosTenantID = $CompanyInfo.id
		$TenantApiHost = $CompanyInfo.apiHost
	} else {
		$SophosTenantID = $false
		Write-PSFMessage -Level Error -Message "Failed to connect to: Sophos (Tenant not found)"
	}

	# Get the Sophos endpoints
	$SophosEndpoints = $false
	$Sophos_Devices = @()
	if ($SophosTenantID -and $TenantApiHost) {
		$SophosHeader = @{
			Authorization = "Bearer $SophosJWT"
			"X-Tenant-ID" = $SophosTenantID
		}
		$SophosEndpoints = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri ($TenantApiHost + "/endpoint/v1/endpoints?pageSize=500")

		$NextKey = $false
		if ($SophosEndpoints.pages.nextKey) {
			$SophosEndpoints.items = [System.Collections.Generic.List[PSCustomObject]]$SophosEndpoints.items
			$NextKey = $SophosEndpoints.pages.nextKey
		}
		while ($NextKey) {
			$SophosEndpoints_NextPage = $false
			$SophosEndpoints_NextPage = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri ($TenantApiHost + "/endpoint/v1/endpoints?pageFromKey=$NextKey")
			foreach ($Endpoint in $SophosEndpoints_NextPage.items) {
				$SophosEndpoints.items.Add($Endpoint)
			}

			$NextKey = $false
			if ($SophosEndpoints_NextPage.pages.nextKey) {
				$NextKey = $SophosEndpoints_NextPage.pages.nextKey
			}
		}

		if (!$SophosEndpoints) {
			Write-PSFMessage -Level Error -Message "Failed to get: Device List from Sophos"
		}
	} else {
		Write-PSFMessage -Level Error -Message "Failed to connect to: Sophos (No Tenant ID or API Host)"
	}

	if ($SophosEndpoints -and $SophosEndpoints.items) {
		$Sophos_Devices = $SophosEndpoints.items | Where-Object { $_.type -eq "computer" -or $_.type -eq "server" }
	} else {
		$Sophos_Devices = @()
		Write-Warning "Warning! Could not get device list from Sophos!"
	}

	############
	# End Sophos device collection
	###########

	# Get RMM devices
	$attempts = 0
	$RMM_Devices = @()
	while (!$RMM_Devices -or $attempts -le 5) {
		$attempts++
		$Response = Set-DrmmApiParameters -Url $DattoAPIKey.URL -Key $DattoAPIKey.Key -SecretKey $DattoAPIKey.SecretKey 6>&1
		if ($RMM_ID) {
			if ($RMM_ID -match "^\d+$") {
				$CompanyInfo = Get-DrmmAccountSites | Where-Object { $_.id -eq $RMM_ID }
				$RMM_ID = $CompanyInfo.uid
			}
			$RMM_Devices = Get-DrmmSiteDevices $RMM_ID | Where-Object { $_.deviceClass -eq 'device' -and $_.deviceType.category -in @("Laptop", "Desktop", "Server") }
		}

		if (!$RMM_Devices) {
			Start-Sleep -Seconds 5
		}
	}

	if (!$RMM_Devices) {
		Write-PSFMessage -Level Error -Message "Failed to get: Device List from RMM"
		Write-PSFMessage -Level Error -Message "Error: $Response"
	}

	# Get RMM device details if using the API (or grab from cache)
	if ($RMM_Devices) {
		if (!(Test-Path -Path $RMMDeviceDetailsLocation)) {
			New-Item -ItemType Directory -Force -Path $RMMDeviceDetailsLocation | Out-Null
		}
		
		$RMMDeviceDetailsPath = "$($RMMDeviceDetailsLocation)\$($Company_Acronym)_rmm_device_details.json"
		$RMMDeviceDetailsCache = @{}
		if (Test-Path $RMMDeviceDetailsPath) {
			$RMMDeviceDetailsCache = Get-Content -Path $RMMDeviceDetailsPath -Raw | ConvertFrom-Json
		}

		$CurrentDate = Get-Date
		$i = 0
		foreach ($Device in $RMM_Devices) {
			$i++
			[int]$PercentComplete = ($i / $RMM_Devices.count * 100)
			Write-Progress -Activity "Getting RMM device details" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "%")
			$Device | Add-Member -NotePropertyName serialNumber -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName manufacturer -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName model -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName MacAddresses -NotePropertyValue @()
			$Device | Add-Member -NotePropertyName memory -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName cpus -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName cpuCores -NotePropertyValue $false
			$Device | Add-Member -NotePropertyName url -NotePropertyValue $false

			$LoadFromCache = $false
			if ($RMMDeviceDetailsCache.($Device.uid)) {
				$CacheAge = $false
				if ($RMMDeviceDetailsCache.($Device.uid).lastUpdated) {
					$CacheAge = New-TimeSpan -Start (Get-Date $RMMDeviceDetailsCache.($Device.uid).lastUpdated) -End $CurrentDate
				}
				if (!$CacheAge -or $CacheAge.Days -ge 7) {
					$RMMDeviceDetailsCache.PSObject.Properties.Remove($Device.uid)
				} else {
					$LoadFromCache = $true
				}
			}

			if (!$LoadFromCache) {
				$AuditDevice = Get-DrmmAuditDevice $Device.uid
				if ($AuditDevice) {
					if (!$RMMDeviceDetailsCache.($Device.uid)) {
						if ($RMMDeviceDetailsCache.GetType().Name -like "Hashtable") {
							$RMMDeviceDetailsCache.($Device.uid) = $false
						} else {
							$RMMDeviceDetailsCache | Add-Member -NotePropertyName $Device.uid -NotePropertyValue $false
						}
					}
					$RMMDeviceDetailsCache.($Device.uid) = $AuditDevice
					if (!$RMMDeviceDetailsCache.($Device.uid).lastUpdated) {
						if ($RMMDeviceDetailsCache.($Device.uid).GetType().Name -like "Hashtable") {
							$RMMDeviceDetailsCache.($Device.uid).lastUpdated = $false
						} else {
							$RMMDeviceDetailsCache.($Device.uid) | Add-Member -NotePropertyName lastUpdated -NotePropertyValue $false
						}
					}
					$RMMDeviceDetailsCache.($Device.uid).lastUpdated = $CurrentDate
				}
			} else {
				$CachedDevice = $RMMDeviceDetailsCache.($Device.uid)
			}
			if ($LoadFromCache -or $AuditDevice) {
				$Nics = if ($LoadFromCache) { $CachedDevice.nics } else { $AuditDevice.nics }
				$Device.serialNumber = if ($LoadFromCache) { $CachedDevice.bios.serialNumber } else { $AuditDevice.bios.serialNumber }
				$Device.manufacturer = if ($LoadFromCache) { $CachedDevice.systemInfo.manufacturer } else { $AuditDevice.systemInfo.manufacturer }
				$Device.model = if ($LoadFromCache) { $CachedDevice.systemInfo.model } else { $AuditDevice.systemInfo.model }
				$Device.MacAddresses = @($Nics | Where-Object { $Nic = $_; $_.macAddress -and ($NetworkAdapterBlacklist | Where-Object { $Nic.instance -like $_ }).Count -eq 0 } | Select-Object instance, macAddress)
				$Device.memory = if ($LoadFromCache) { $CachedDevice.systemInfo.totalPhysicalMemory } else { $AuditDevice.systemInfo.totalPhysicalMemory }
				$Device.cpus = if ($LoadFromCache) { $CachedDevice.processors } else { $AuditDevice.processors }
				$Device.cpuCores = if ($LoadFromCache) { $CachedDevice.systemInfo.totalCpuCores } else { $AuditDevice.systemInfo.totalCpuCores }
				$Device.url = if ($LoadFromCache) { $CachedDevice.portalUrl } else { $AuditDevice.portalUrl }
			}
		}

		if ($RMMDeviceDetailsCache -and $RMMDeviceDetailsPath) {
			$RMMDeviceDetailsCache | ConvertTo-Json -Depth 8 | Out-File -FilePath $RMMDeviceDetailsPath
		}
		Write-Progress -Activity "Getting RMM device details" -Status "Ready" -Completed
	}

	# Get all devices from ITG
	$ITG_Devices = @()
	if ($ITGConnected -and $ITG_ID) {
		$ITG_Devices = Get-ITGlueConfigurations -page_size "1000" -organization_id $ITG_ID
		$i = 1
		while ($ITG_Devices.links.next) {
			$i++
			$Configurations_Next = Get-ITGlueConfigurations -page_size "1000" -page_number $i -organization_id $ITG_ID
			if (!$Configurations_Next -or $Configurations_Next.Error) {
				# We got an error querying configurations, wait and try again
				Start-Sleep -Seconds 2
				$Configurations_Next = Get-ITGlueConfigurations -page_size "1000" -page_number $i -organization_id $ITG_ID
		
				if (!$Configurations_Next -or $Configurations_Next.Error) {
					Write-PSFMessage -Level Error -Message "An error occurred trying to get the existing configurations from ITG. Exiting..."
					Write-PSFMessage -Level Error -Message $Configurations_Next.Error
					exit 1
				}
			}
			$ITG_Devices.data += $Configurations_Next.data
			$ITG_Devices.links = $Configurations_Next.links
		}
		if ($ITG_Devices -and $ITG_Devices.data) {
			$ITG_Devices = $ITG_Devices.data
		}
		if (!$ITG_Devices) {
			Write-Warning "There was an issue getting the Configurations from ITG. Skipping org..."
			continue
		}
	}
	$ITG_DevicesHash = @{}
	foreach ($Device in $ITG_Devices) { 
		$ITG_DevicesHash[$Device.id] = $Device
	}

	# Get all devices from Azure & Intune
	$Azure_Devices = @()
	$Intune_Devices = @()
	if ($AzureConnected) {
		try {
			$Azure_Devices = Get-MgDevice -All | Where-Object { $_.OperatingSystem -notin @("Android", "iOS") }
		} catch {
			Write-Warning "GDAP is not properly setup. Could not query Azure devices."
			$Azure_Devices = @()
		}
		try {
			$Intune_Devices = Get-MgDeviceManagementManagedDevice | Where-Object { $_.OperatingSystem -notin @("Android", "iOS") }
		} catch {
			$Intune_Devices = @()
		}
	}
	$Azure_DevicesHash = @{}
	$Intune_DevicesHash = @{}
	foreach ($Device in $Azure_Devices) { 
		$Azure_DevicesHash[$Device.id] = $Device
	}
	foreach ($Device in $Intune_Devices) { 
		$Intune_DevicesHash[$Device.Id] = $Device
	}

	# Get all devices from JumpCloud
	$JC_Devices = @()
	if ($JCConnected) {
		Write-Host "Getting JC System"
		$JC_Devices = Get-JCSystem | Where-Object { $_.desktopCapable }
	}
	$JC_DevicesHash = @{}
	foreach ($Device in $JC_Devices) { 
		$JC_DevicesHash[$Device.id] = $Device
	}

	$JC_Users = @()
	if (($JC_Devices | Measure-Object).Count -gt 0) {
		Write-Host "Getting JC Users"
		foreach ($Device in $JC_Devices) {
			$UserInfo = Get-JCSystemUser -SystemID $Device.id
			if ($UserInfo) {
				$JC_Users += $UserInfo
			}
		}
	}

	Write-Output "Imported all devices."
	Write-Output "===================="

	# Filter Screen Connect Devices
	if ($SC_Company.GetType().Name -like "String") {
		$SC_Devices = $SC_Devices_Full | Where-Object { $_.CustomProperty1 -like $SC_Company }
	} else {
		$SC_Devices_Temp = @()
		foreach ($Company in $SC_Company) {
			$SC_Devices_Temp += $SC_Devices_Full | Where-Object { $_.CustomProperty1 -like $Company }
		}
		$SC_Devices = $SC_Devices_Temp
	}

	# Filter columns to only the one's we want
	$SC_Devices = $SC_Devices | Select-Object SessionID, Name, GuestMachineName, GuestMachineSerialNumber, GuestHardwareNetworkAddress, 
												@{Name="DeviceType"; E={if ($_.GuestOperatingSystemName -like "*Server*") { "Server" } else { "Workstation" } }}, 
												@{Name="GuestLastActivityTime"; E={Convert-UTCtoLocal($_.GuestLastActivityTime)}}, @{Name="GuestInfoUpdateTime"; E={Convert-UTCtoLocal($_.GuestInfoUpdateTime)}}, @{Name="GuestLastBootTime"; E={Convert-UTCtoLocal($_.GuestLastBootTime)}},
												GuestLoggedOnUserName, GuestLoggedOnUserDomain, GuestOperatingSystemName, GuestMachineManufacturerName, GuestMachineModel, GuestMachineDescription, GuestSystemMemoryTotalMegabytes, GuestProcessorName, GuestProcessorVirtualCount
	# Sometimes the LastActivityTime field is not set even though the device is on, in these cases it's set to Year 1 
	# Also, if a computer is online but inactive, the infoupdate time can be more recent and a better option
	# We'll create a new GuestLastSeen property here that is the most recent date of the 3 available
	$SC_Devices | Add-Member -NotePropertyName GuestLastSeen -NotePropertyValue $null
	$SC_Devices | ForEach-Object { 
		$MostRecentDate = @($_.GuestLastActivityTime, $_.GuestInfoUpdateTime, $_.GuestLastBootTime) | Sort-Object | Select-Object -Last 1
		$_.GuestLastSeen = $MostRecentDate
	}
	$SC_DevicesHash = @{}
	foreach ($Device in $SC_Devices) { 
		$SC_DevicesHash[$Device.SessionID] = $Device
	}

	$RMM_Devices = $RMM_Devices |
						Select-Object @{Name="Device UID"; E={$_.uid}}, @{Name="Device Hostname"; E={$_.hostname}}, @{Name="Serial Number"; E={$_.serialNumber}}, MacAddresses, 
										@{Name="Device Type"; E={$_.deviceType.category}}, @{Name="Status"; E={$_.online}}, @{Name="Last Seen"; E={ if ($_.online -eq "True") { Get-Date } else { Convert-UTCtoLocal(([datetime]'1/1/1970').AddMilliseconds($_.lastSeen)) } }}, 
										extIpAddress, intIpAddress,
										@{Name="Last User"; E={$_.lastLoggedInUser}}, Domain, @{Name="Operating System"; E={$_.operatingSystem}}, 
										Manufacturer, @{Name="Device Model"; E={$_.model}}, @{Name="Warranty Expiry"; E={$_.warrantyDate}}, @{Name="Device Description"; E={$_.description}}, 
										memory, cpus, cpuCores, url, @{Name="Antivirus"; E={$_.antivirus.antivirusProduct}},
										@{Name="ScreenConnectID"; E={
											$SC = $_.udf.udf13;
											if ($SC -and $SC -like "*$($SCLogin.URL.TrimStart('http').TrimStart('s').TrimStart('://'))*") {
												$Found = $SC -match '\/\/((\{){0,1}[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\}){0,1})\/Join'
												if ($Found -and $Matches[1]) {
													$Matches[1]
												}
											}
										}}, @{Name="ToDelete"; E={ if ($_.udf.udf30 -eq "True") { $true } else { $false } }}, suspended
	$RMM_DevicesHash = @{}
	foreach ($Device in $RMM_Devices) { 
		$RMM_DevicesHash[$Device."Device UID"] = $Device
	}
	
	$Sophos_Devices = $Sophos_Devices | Select-Object id, @{Name="hostname"; E={$_.hostname -replace '[^\x00-\x7F]+', ''}}, macAddresses, 
											@{Name="type"; E={if ($_.type -eq "computer") { "Workstation"} else { "Server" }}}, 
											lastSeenAt, @{Name="LastUser"; E={($_.associatedPerson.viaLogin -split '\\')[1]}}, @{Name="OS"; E={if ($_.os.name) { $_.os.name } else { "$($_.os.platform) $($_.os.majorVersion).$($_.os.minorVersion)" }}}
	$Sophos_DevicesHash = @{}
	foreach ($Device in $Sophos_Devices) { 
		$Sophos_DevicesHash[$Device.id] = $Device
	}

	foreach ($Device in $Sophos_Devices) {
		$Device | Add-Member -NotePropertyName webID -NotePropertyValue $false
		$EndpointID = $Device.id
		$WebEndpointID = convert_sophos_id_to_web $EndpointID
		$Device.webID = $WebEndpointID
	}

	##############
	# Get Matches
	##############
	if ($MatchedDevicesLocation) {
		if (!(Test-Path -Path $MatchedDevicesLocation)) {
			New-Item -ItemType Directory -Force -Path $MatchedDevicesLocation | Out-Null
		}
		$Day = Get-Date -Format "dd"
		$Month = Get-Date -Format "MM"
		$Year = Get-Date -Format "yyyy"

		$MatchedDevicesJsonPath = "$($MatchedDevicesLocation)\$($Company_Acronym)_matched_devices_$($Year)_$($Month)_$($Day).json"
		if ($MatchedDevicesJsonPath -and (Test-Path -Path $MatchedDevicesJsonPath)) {
			$MatchedDevices = Get-Content -Path $MatchedDevicesJsonPath -Raw | ConvertFrom-Json
			if ($MatchedDevices -and ($MatchedDevices | Measure-Object).Count -eq 0) {
				Write-Warning "Could not find any matched devices for $Company_Acronym."
				continue
			}
		} else {
			Write-Warning "Could not find any matched devices for $Company_Acronym."
			continue
		}
	} else {
		Write-Warning "`$MatchedDevicesLocation needs to be set. Skipping $Company_Acronym..."
		continue
	}

	##############
	# Cleanup
	##############

	# Get a list of Autotask tickets related to RMM, SC, or Sophos agent issues
	$RepairTickets = Get-AutotaskAPIResource -Resource Tickets -SearchQuery '{"filter":[{"op":"noteq","field":"Status","value":5},{"op":"or","items": [{"op":"contains","field":"title","value":"RMM"},{"op":"contains","field":"title","value":"SC"},{"op":"contains","field":"title","value":"ScreenConnect"},{"op":"contains","field":"title","value":"Sophos"}]}]}'
	$RepairTickets = $RepairTickets | Where-Object {
		$_.title -like "*Reinstall RMM*" -or $_.title -like "*Install RMM*" -or $_.title -like"*Audit RMM*" -or $_.title -like "*Troubleshoot RMM*" -or $_.title -like "*Repair RMM*" -or
		$_.title -like "*Reinstall SC*" -or $_.title -like "*Install SC*" -or $_.title -like"*Audit SC*" -or $_.title -like "*Troubleshoot SC*" -or $_.title -like "*Repair SC*" -or
		$_.title -like "*Reinstall ScreenConnect*" -or $_.title -like "*Install ScreenConnect*" -or $_.title -like"*Audit ScreenConnect*" -or $_.title -like "*Troubleshoot ScreenConnect*" -or $_.title -like "*Repair ScreenConnect*" -or
		$_.title -like "*Reinstall Sophos*" -or $_.title -like "*Install Sophos*" -or $_.title -like"*Audit Sophos*" -or $_.title -like "*Troubleshoot Sophos*" -or $_.title -like "*Repair Sophos*"
	}

	# Get the existing log
	$LogFilePath = "$($LogLocation)\$($Company_Acronym)_log.json"
	if ($LogLocation -and (Test-Path -Path $LogFilePath)) {
		$LogHistory = Get-Content -Path $LogFilePath -Raw | ConvertFrom-Json
	} else {
		$LogHistory = @{}
	}

	# Prepare install queue
	if (!(Test-Path -Path $InstallQueueLocation)) {
		New-Item -ItemType Directory -Force -Path $InstallQueueLocation | Out-Null
	}
	$InstallQueuePath = "$($InstallQueueLocation)\$($Company_Acronym)_install_queue.json"

	# Prepare recent RMM jobs log
	if (!(Test-Path -Path $RecentRMMJobsLocation)) {
		New-Item -ItemType Directory -Force -Path $RecentRMMJobsLocation | Out-Null
	}
	$RecentRMMJobsPath = "$($RecentRMMJobsLocation)\$($Company_Acronym)_recent_rmm_jobs.json"

	# Find any duplicates that need to be removed
	if ($DODuplicateSearch) {
		Write-Host "Searching for duplicates..."
		$Duplicates = @()
		foreach ($Device in $MatchedDevices) {
			if ($Device.sc_matches.count -gt 1 -or $Device.rmm_matches.count -gt 1 <# -or $Device.sophos_matches.count -gt 1 #> -or ($JCConnected -and $Device.jc_matches.count -gt 1)) { 
				$Duplicates += $Device
			}
		}

		if (($Duplicates | Measure-Object).count -gt 0) {
			$DuplicatesTable = @()

			foreach ($Device in $Duplicates) {
				# ScreenConnect
				if ($Device.sc_matches.count -gt 1) {
					$OrderedDevices = compare_activity_sc($Device.sc_matches)
					$i = 0
					foreach ($OrderedDevice in $OrderedDevices) {
						$SCDevice = $SC_DevicesHash[$OrderedDevice.id]
						$Deleted = $false
						$AllowDeletion = $true
						if ($DontAutoDelete -and ($DontAutoDelete.Hostnames -contains $SCDevice.Name -or $DontAutoDelete.SC -contains $SCDevice.Name -or $DontAutoDelete.SC -contains $OrderedDevice.id)) {
							$AllowDeletion = $false
						}
						if ($i -gt 0 -and !$ReadOnly -and $AllowDeletion) {
							$Deleted = delete_from_sc -SC_ID $OrderedDevice.id  -SCWebSession $SCWebSession
							if ($Deleted) {
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "sc" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $OrderedDevice.id -Sophos_Device_ID $Device.sophos_matches -ChangeType "delete" -Hostname $SCDevice.Name -Reason "Duplicate"
							}
						}
						$DuplicatesTable += [PsCustomObject]@{
							type = "SC"
							hostname = $SCDevice.Name
							id = $OrderedDevice.id 
							last_active = $OrderedDevice.last_active
							remove = if ($i -eq 0) { "No" } else { "Yes" }
							auto_deleted = $Deleted
							link = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevice.Name)/$($OrderedDevice.id)"
						}
						$i++
					}
				}

				# RMM
				if ($Device.rmm_matches.count -gt 1) {
					$OrderedDevices = compare_activity_rmm($Device.rmm_matches)
					$i = 0
					foreach ($OrderedDevice in $OrderedDevices) {
						$RMMDevice = $RMM_DevicesHash[$OrderedDevice.id]
						$Deleted = $false
						$AllowDeletion = $true
						if ($DontAutoDelete -and ($DontAutoDelete.Hostnames -contains $RMMDevice."Device Hostname" -or $DontAutoDelete.RMM -contains $RMMDevice."Device Hostname" -or $DontAutoDelete.RMM -contains $OrderedDevice.id)) {
							$AllowDeletion = $false
						}
						if ($i -gt 0 -and !$ReadOnly -and !$RMMDevice.ToDelete -and $AllowDeletion) {
							delete_from_rmm -RMM_Device_ID $OrderedDevice.id
							$Deleted = "Set DeleteMe UDF"
							log_change -Company_Acronym $Company_Acronym -ServiceTarget "rmm" -RMM_Device_ID $OrderedDevice.id -SC_Device_ID $Device.sc_matches -Sophos_Device_ID $Device.sophos_matches  -ChangeType "delete" -Hostname $RMMDevice."Device Hostname" -Reason "Duplicate"
						} elseif ($RMMDevice.ToDelete) {
							$Deleted = "Pending Deletion"
						}
						if ($RMMDevice.url) {
							$EmailLink = $RMMDevice.url
						} else {
							$EmailLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($OrderedDevice.id)"
						}
						$DuplicatesTable += [PsCustomObject]@{
							type = "RMM"
							hostname = $RMMDevice."Device Hostname"
							id = $OrderedDevice.id 
							last_active = $OrderedDevice.last_active
							remove = if ($i -eq 0) { "No" } else { "Yes" }
							auto_deleted = $Deleted
							link = $EmailLink
						}
						$i++
					}
				}

				# Sophos (Disabled. Sophos really doesn't like you deleting duplicates so best to just leave them)
				<# if ($Device.sophos_matches.count -gt 1) {
					$OrderedDevices = compare_activity_sophos($Device.sophos_matches)
					$i = 0
					foreach ($OrderedDevice in $OrderedDevices) {
						$SophosDevice = $Sophos_DevicesHash[$OrderedDevice.id]
						$DuplicatesTable += [PsCustomObject]@{
							type = "Sophos"
							hostname = $SophosDevice.hostname
							id = $OrderedDevice.id 
							last_active = $OrderedDevice.last_active
							remove = if ($i -eq 0) { "No" } else { "Yes" }
							auto_deleted = $false
							link = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevice.webID)"
						}
						$i++
					}
				} #>

				# JumpCloud
				if ($JCConnected -and $Device.jc_matches.count -gt 1) {
					$OrderedDevices = compare_activity_jc($Device.jc_matches)
					$i = 0
					foreach ($OrderedDevice in $OrderedDevices) {
						$JCDevice = $JC_DevicesHash[$OrderedDevice.id]
						$Deleted = $false
						$AllowDeletion = $true
						if ($DontAutoDelete -and (($JCDevice.hostname -and $DontAutoDelete.Hostnames -contains $JCDevice.hostname) -or $DontAutoDelete.Hostnames -contains $JCDevice.displayName -or ($JCDevice.hostname -and $DontAutoDelete.JC -contains $JCDevice.hostname) -or $DontAutoDelete.JC -contains $JCDevice.displayName -or $DontAutoDelete.JC -contains $OrderedDevice.id)) {
							$AllowDeletion = $false
						}
						if ($i -gt 0 -and !$ReadOnly -and $AllowDeletion) {
							$Deleted = delete_from_jc -JC_ID $OrderedDevice.id
							if ($Deleted) {
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "jc" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $OrderedDevice.id -Sophos_Device_ID $Device.sophos_matches -JC_Device_ID $Device.jc_matches -ChangeType "delete" -Hostname if ($JCDevice.hostname) { $JCDevice.hostname } else { $JCDevice.displayName } -Reason "Duplicate"
							}
						}
						$DuplicatesTable += [PsCustomObject]@{
							type = "JC"
							hostname = if ($JCDevice.hostname) { $JCDevice.hostname } else { $JCDevice.displayName }
							id = $OrderedDevice.id 
							last_active = $OrderedDevice.last_active
							remove = if ($i -eq 0) { "No" } else { "Yes" }
							auto_deleted = $Deleted
							link = "https://console.jumpcloud.com/#/devices/$($JCDevice.id)/details"
						}
						$i++
					}
				}
			}

			Write-Host "Warning! Duplicates were found!" -ForegroundColor Red

			# Now remove duplicates from $MatchedDevices as we can ignore them for the rest of this script
			$UpdatedMatchedDevices = $false
			foreach ($Device in $DuplicatesTable) {
				if ($Device.Remove -eq "No") {
					continue
				}

				if ($Device.type -eq "SC") {
					$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.sc_matches }
					$SCDevices = @()
					foreach ($DeviceID in $MatchedDevice.sc_matches) {
						$SCDevices += $SC_DevicesHash[$DeviceID]
					}
					foreach ($MDevice in $MatchedDevice) {
						$MDevice.sc_matches = $MatchedDevice.sc_matches | Where-Object { $_ -ne $Device.id }
						$MDevice.sc_hostname = @($SCDevices.Name)
					}
					if (($MatchedDevice | Measure-Object).Count -gt 0) {
						$UpdatedMatchedDevices = $true
					}
				}

				if ($Device.type -eq "RMM") {
					$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.rmm_matches }
					$RMMDevices = @()
					foreach ($DeviceID in $MatchedDevice.rmm_matches) {
						$RMMDevices += $RMM_DevicesHash[$DeviceID]
					}
					foreach ($MDevice in $MatchedDevice) {
						$MDevice.rmm_matches = $MatchedDevice.rmm_matches | Where-Object { $_ -ne $Device.id }
						$MDevice.rmm_hostname = @($RMMDevices."Device Hostname")
					}
					if (($MatchedDevice | Measure-Object).Count -gt 0) {
						$UpdatedMatchedDevices = $true
					}
				}

				if ($Device.type -eq "Sophos") {
					$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.sophos_matches }
					$SophosDevices = @()
					foreach ($DeviceID in $MatchedDevice.sophos_matches) {
						$SophosDevices += $Sophos_DevicesHash[$DeviceID]
					}
					foreach ($MDevice in $MatchedDevice) {
						$MDevice.sophos_matches = $MatchedDevice.sophos_matches | Where-Object { $_ -ne $Device.id }
						$MDevice.sophos_hostname = @($SophosDevices.hostname)
					}
					if (($MatchedDevice | Measure-Object).Count -gt 0) {
						$UpdatedMatchedDevices = $true
					}
				}

				if ($Device.type -eq "JC") {
					$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.jc_matches }
					$JCDevices = @()
					foreach ($DeviceID in $MatchedDevice.jc_matches) {
						$JCDevices += $JC_DevicesHash[$DeviceID]
					}
					foreach ($MDevice in $MatchedDevice) {
						$MDevice.jc_matches = $MatchedDevice.jc_matches | Where-Object { $_ -ne $Device.id }
						$MDevice.jc_hostname = @($JCDevices | Foreach-Object { if ($_.hostname) { $_.hostname } else { $_.displayName } })
					}
					if (($MatchedDevice | Measure-Object).Count -gt 0) {
						$UpdatedMatchedDevices = $true
					}
				}
			}

			if ($UpdatedMatchedDevices -and $MatchedDevicesJsonPath) {
				# Update the exported MatchedDevices json file
				$MatchedDevices | ConvertTo-Json | Out-File -FilePath $MatchedDevicesJsonPath
			}
		}

		Write-Host "Duplicate check complete."
		Write-Host "======================"
	}

	# Check for devices that look like they should not be under this company
	if ($DOWrongCompanySearch) {
		Write-Host "Searching for any devices that look like they belong in a different company..."
		$MoveDevices = @()

		:matchedDeviceLoop foreach ($Device in $MatchedDevices) {
			$Hostnames = @()
			$SCDeviceIDs = @($Device.sc_matches)
			$RMMDeviceIDs = @($Device.rmm_matches)
			$SophosDeviceIDs = @($Device.sophos_matches)

			$DeviceType = $false
			$OperatingSystem = $false

			foreach ($Acronym in $Device_Acronyms) {
				if ($Device.sc_hostname -like "$($Acronym)-*" -or $Device.rmm_hostname -like "$($Acronym)-*" -or $Device.sophos_hostname -like "$($Acronym)-*") {
					continue matchedDeviceLoop
				}
			}

			if ($SCDeviceIDs) {
				$SCDevices = @()
				foreach ($DeviceID in $SCDeviceIDs) {
					$SCDevices += $SC_DevicesHash[$DeviceID]
				}			
				$Hostnames += $SCDevices.Name
				$OperatingSystem = $SCDevices[0].GuestOperatingSystemName
				$DeviceType = $SCDevices[0].DeviceType
			}
			if ($RMMDeviceIDs) {
				$RMMDevices = @()
				foreach ($DeviceID in $RMMDeviceIDs) {
					$RMMDevices += $RMM_DevicesHash[$DeviceID]
				}
				$Hostnames += $RMMDevices."Device Hostname"
				if (!$OperatingSystem) {
					$OperatingSystem = $RMMDevices[0]."Operating System"
				}
				if ($DeviceType) {
					$DeviceType = $RMMDevices[0]."Device Type"
				}
			}
			if ($SophosDeviceIDs) {
				$SophosDevices = @()
				foreach ($DeviceID in $SophosDeviceIDs) {
					$SophosDevices += $Sophos_DevicesHash[$DeviceID]
				}
				$Hostnames += $SophosDevices.hostname
				if (!$OperatingSystem) {
					$OperatingSystem = $SophosDevices[0].OS
				}
				if (!$DeviceType) {
					$DeviceType = $SophosDevices[0].type
				}
			}
			$Hostnames = $HostNames | Sort-Object -Unique

			if (!$DeviceType) {
				if ($OperatingSystem -and $OperatingSystem -like "*Server*") {
					$DeviceType = "Server"
				} else {
					$DeviceType = "Workstation"
				}
			}

			# Ignore Macs
			if ($OperatingSystem -like "Mac OS*" -or $OperatingSystem -like "macOS*") {
				continue
			}

			# Check if the hostname uses this customers acronym
			$GoodHostname = $false
			foreach ($Acronym in $Device_Acronyms) {
				if ((($Hostnames | Where-Object { $_ -like "$($Acronym)-*" }) | Measure-Object).Count -gt 0) {
					$GoodHostname = $true
					continue
				}
			}

			if (!$GoodHostname -and (($Hostnames | Where-Object { $_ -in $Device_Whitelist }) | Measure-Object).Count -gt 0) {
				$GoodHostname = $true
			}

			if ($GoodHostname) {
				continue
			}

			# Check for default hostnames and exceptions
			if ((($Hostnames | Where-Object { $_ -notlike "*-*" -or $_ -like "DESKTOP-*" -or $_ -like "LAPTOP-*" -or $_ -like "*MacBook*" -or  $_ -like "STS-*" }) | Measure-Object).Count -gt 0) {
				continue
			}

			# Ignore servers
			if ($DeviceType -eq "Server") {
				continue
			}

			$SCLink = ""
			$RMMLink = ""
			$SophosLink = ""
			if ($SCDeviceIDs) {
				$SCLink = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevices[0].Name)/$($SCDeviceIDs[0])"
			}
			if ($RMMDeviceIDs) {
				if ($RMMDevices[0].url) {
					$RMMLink = $RMMDevices[0].url
				} else {
					$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceIDs[0])"
				}
			}
			if ($SophosDeviceIDs) {
				$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevices[0].webID)"
			}
			$RMMAntivirus = $false
			if (!$SophosDeviceIDs -and $RMMDeviceIDs) {
				$RMMAntivirus = $RMMDevices[0].Antivirus
			}

			# This device does not look like it belongs here, lets flag it
			$MoveDevices += [PsCustomObject]@{
				Hostnames = $Hostnames -join ', '
				DeviceType = $DeviceType
				InSC = if ($SCDeviceIDs) { "Yes" } else { "No" }
				InRMM = if ($RMMDeviceIDs) { "Yes" } else { "No" }
				InSophos = if ($SophosDeviceIDs) { "Yes" } elseif ($RMMAntivirus -and $RMMAntivirus -like "Sophos*") { "Yes, missing from portal" } else { "No" }
				SC_Link = $SCLink
				RMM_Link = $RMMLink
				Sophos_Link = $SophosLink
			}
		}

		if (($MoveDevices | Where-Object { $_.Hostnames -notlike "STS-*" } | Measure-Object).count -gt 0) { # ignore sts loaners
			Write-Host "Warning! Devices found that may not belong in this company!" -ForegroundColor Red

			# Try sending an email
			$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
			$EmailChangeType = "wrong-company-email"
			$Reason = "Email sent for devices found under wrong company"

			$EmailQuery_Params = @{
				LogHistory = $LogHistory
				StartTime = $TwoWeeksAgo
				EndTime = 'now'
				ServiceTarget = 'email'
				ChangeType = $EmailChangeType
				Reason = $EmailReason
			}
			$History_Subset_Emails = log_query @EmailQuery_Params

			# no emails were already sent, continue
			if (($History_Subset_Emails | Measure-Object).count -eq 0) {
				# send an email
				$EmailSubject = "Device Audit on $Company_Acronym - Devices Found that belong to a different company"
				$EmailIntro = "Devices were found that look like they may not belong in this company. Please review:"
				$DeviceTable = @($MoveDevices) | Sort-Object Hostnames,DeviceType | ConvertTo-HTML -Fragment -As Table | Out-String

				$HTMLEmail = $EmailTemplate -f `
								$EmailIntro, 
								"Devices Found with odd Hostnames", 
								$DeviceTable, 
								"<br />Please review these devices manually. Either fix them or update the script's config file to whitelist them."

				$mailbody = @{
					"From" = $EmailFrom
					"To" = $EmailTo_FailedFixes
					"Subject" = $EmailSubject
					"HTMLContent" = $HTMLEmail
				} | ConvertTo-Json -Depth 6

				$headers = @{
					'x-api-key' = $Email_APIKey.Key
				}

				Invoke-RestMethod -Method Post -Uri $Email_APIKey.Url -Body $mailbody -Headers $headers -ContentType application/json
				Write-Host "Email Sent." -ForegroundColor Yellow

				# log the sent email
				log_change -Company_Acronym $Company_Acronym -ServiceTarget 'email' -RMM_Device_ID "" -SC_Device_ID "" -Sophos_Device_ID "" -ChangeType $EmailChangeType -Hostname "" -Reason $EmailReason
			}
		}

		Write-Host "Check for devices that don't belong is complete."
		Write-Host "======================"
	}

	# Get activity comparisons and store for later (so we aren't repeating this over and over)
	if ($DOBrokenConnectionSearch -or $DOMissingConnectionSearch -or $DOInactiveSearch -or $DOUsageDBSave -or $DOBillingExport) {
		foreach ($Device in $MatchedDevices) {
			$ActivityComparison = compare_activity($Device)
			$ActivityComparison.Values = @($ActivityComparison.Values)
			if (!$Device.activity_comparison) {
				$Device | Add-Member -NotePropertyName activity_comparison -NotePropertyValue $null
			}
			$Device.activity_comparison = $ActivityComparison
		}
	}

	$MatchedDevicesHash = @{}
	foreach ($Device in $MatchedDevices) { 
		$MatchedDevicesHash[$Device.id] = $Device
	}

	# Find broken device connections (e.g. online recently in ScreenConnect but not in RMM)
	if ($DOBrokenConnectionSearch) {
		Write-Host "Searching for broken connections..."
		$BrokenConnections = @()
		$SendEmail = $false
		foreach ($Device in $MatchedDevices) {
			$ActivityComparison = $Device.activity_comparison
			$Activity = $ActivityComparison.Values | Sort-Object last_active

			if (($Activity | Measure-Object).count -gt 1) {
				$LastIndex = ($Activity | Measure-Object).count-1
				$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
				for ($i=0; $i -lt $LastIndex; $i++) {
					$ComparisonDate = [DateTime]$Activity[$i].last_active
					$Timespan = New-TimeSpan -Start $ComparisonDate -End $NewestDate

					if ($Timespan.Days -gt $BrokenThreshold) {
						$Hostname = ''
						$DeviceType = $Activity[$i].type
						$DeviceID = $Activity[$i].id
						if ($DeviceType -eq 'sc') {
							$Hostname = ($SC_DevicesHash[$DeviceID]).Name
						} elseif ($DeviceType -eq 'rmm') {
							$RMMDevice = $RMM_DevicesHash[$DeviceID]
							$Hostname = $RMMDevice."Device Hostname"
						} elseif ($DeviceType -eq 'sophos') {
							$SophosDevice = $Sophos_DevicesHash[$DeviceID]
							$Hostname = $SophosDevice.hostname
						} elseif ($DeviceType -eq 'azure') {
							$AzureDevice = $Azure_DevicesHash[$DeviceID]
							$Hostname = $AzureDevice.DisplayName
						} elseif ($DeviceType -eq 'intune') {
							$InTuneDevice = $InTune_DevicesHash[$DeviceID]
							$Hostname = $InTuneDevice.DeviceName
						}

						$Link = ''
						if ($DeviceType -eq 'sc') {
							$Link = "$($SCLogin.URL)/Host#Access/All%20Machines/$($Hostname)/$($DeviceID)"
						} elseif ($DeviceType -eq 'rmm') {
							if ($RMMDevice.url) {
								$Link = $RMMDevice.url
							} else {
								$Link = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($DeviceID)"
							}
						} elseif ($DeviceType -eq 'sophos') {
							$Link = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevice.webID)"
						} else {
							$Link = ""
						}

						# See if we can try to automatically fix this issue
						$AutoFix = $false
						if (!$ReadOnly -and $DeviceType -eq 'rmm' -and $RMM_ID -and ($Device.sc_matches | Measure-Object).Count -gt 0 -and $SCLogin.Username -and $SCWebSession -and $Device.id -notin $MoveDevices.ID) {
							# Device broken in rmm but is in SC and we are using the SC api account, have the rmm org id, and this device is not in the $MoveDevices array (devices that look like they dont belong)
							$SC_Device = @()
							foreach ($DeviceID in $Device.sc_matches) {
								$SC_Device += $SC_DevicesHash[$DeviceID]
							}
							
							# Only continue if the device was seen recently in SC (this will only work if it is active) and is using Windows
							foreach ($SCDevice in $SC_Device) {
								$LogParams = @{
									ServiceTarget = "sc"
									SC_Device_ID = $SCDevice.SessionID
									ChangeType = "install_rmm"
									Hostname = $SCDevice.Name
								}
								$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
								$EmailError = "RMM is broken on $($LogParams.Hostname). The Device Audit script has tried to reinstall RMM via SC $AttemptCount times now but it has not succeeded."
								$LogParams.RMM_Device_ID = $Device.rmm_matches
								$LogParams.Sophos_Device_ID = $Device.sophos_matches
								$LogParams.Reason = "RMM connection broken"

								if ($SCDevice.GuestOperatingSystemName -like "*Windows*" -and $SCDevice.GuestOperatingSystemName -notlike "*Windows Embedded*") {
									if ($SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
										if (install_rmm_using_sc -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
											$AutoFix = $true
											check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
											log_change @LogParams -Company_Acronym $Company_Acronym
										}
									} else {
										add_device_to_install_queue -SC_ID $SCDevice.SessionID -ToInstall 'rmm'
									}
								} elseif ($SCDevice.GuestOperatingSystemName -like "*Mac OS*") {
									if ($SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
										if (install_rmm_using_sc_mac -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
											$AutoFix = $true
											check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
											log_change @LogParams -Company_Acronym $Company_Acronym
										}
									} else {
										add_device_to_install_queue -SC_ID $SCDevice.SessionID -ToInstall 'rmm'
									}
								}
							}
						}

						if (!$ReadOnly -and $DeviceType -eq 'sc' -and $RMM_ID -and ($Device.rmm_matches | Measure-Object).Count -gt 0 -and $Device.id -notin $MoveDevices.ID) {
							# Device broken in sc but is in RMM and we are using the rmm api, and this device is not in the $MoveDevices array (devices that look like they dont belong)
							$RMM_Device = @()
							foreach ($DeviceID in $Device.rmm_matches) {
								$RMM_Device += $RMM_DevicesHash[$DeviceID]
							}
	
							# Only continue if the device was seen in RMM in the last 24 hours
							foreach ($RMMDevice in $RMM_Device) {
								if ($RMMDevice.suspended -ne "True") {
									if ($RMMDevice.Status -eq "Online" -or $RMMDevice."Last Seen" -eq "Currently Online" -or ($RMMDevice."Last Seen" -as [DateTime]) -gt (Get-Date).AddHours(-24)) {
										$LogParams = @{
											ServiceTarget = "rmm"
											RMM_Device_ID = $RMMDevice."Device UID"
											ChangeType = "install_sc"
											Hostname = $RMMDevice."Device Hostname"
										}
										$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory

										if ($AttemptCount -gt 3) {
											uninstall_sc_using_rmm -RMM_Device $RMMDevice
										}
										if (install_sc_using_rmm -RMM_Device $RMMDevice) {
											$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
											$EmailError = "ScreenConnect is broken on $($LogParams.Hostname). The Device Audit script has tried to reinstall SC via RMM $AttemptCount times now but it has not succeeded."
											$LogParams.SC_Device_ID = $Device.sc_matches
											$LogParams.Sophos_Device_ID = $Device.sophos_matches
											$LogParams.Reason = "SC connection broken"
		
											$AutoFix = $true
											check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
											log_change @LogParams -Company_Acronym $Company_Acronym
										}
									} else {
										add_device_to_install_queue -RMM_ID $RMMDevice."Device UID" -ToInstall 'sc'
									}
								}
							}
						}
						
						$RepairTickets_Subset = @()
						if ($DeviceType -in @("rmm", "sc", "sophos")) {
							$RepairTickets_Subset = repair_tickets -ServiceTarget $DeviceType -Hostname $Hostname
						}

						if (!$AutoFix -and $Device.id -notin $MoveDevices.ID -and $Timespan.Days -le 7 -and ($RepairTickets_Subset | Measure-Object).Count -eq 0) {
							$SendEmail = $true
						}

						$BrokenConnections += [PsCustomObject]@{
							BrokenType = $DeviceType
							Hostname = $Hostname
							LastActive = $Activity[$i].last_active
							AutoFix_Attempted = $AutoFix
							SC_Time = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].last_active } else { "NA" }
							RMM_Time = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].last_active } else { "NA" }
							Sophos_Time = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].last_active } else { "NA" }
							RepairTicket = if (($RepairTickets_Subset | Measure-Object).Count -gt 0) { $RepairTickets_Subset.ticketNumber -join ", " } else { "NA" }
							Link = $Link
						}
					}
				}
			}
		}

		if ($SendEmail) {
			Write-Host "Warning! Broken connections were found!" -ForegroundColor Red

			# Try sending an email
			$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
			$EmailChangeType = "broken-connections-email"
			$Reason = "Email sent for devices found with a broken connection"

			$EmailQuery_Params = @{
				LogHistory = $LogHistory
				StartTime = $TwoWeeksAgo
				EndTime = 'now'
				ServiceTarget = 'email'
				ChangeType = $EmailChangeType
				Reason = $EmailReason
			}
			$History_Subset_Emails = log_query @EmailQuery_Params

			# no emails were already sent, continue
			if (($History_Subset_Emails | Measure-Object).count -eq 0) {
				# send an email
				$EmailSubject = "Device Audit on $Company_Acronym - Devices Found with a broken connection"
				$EmailIntro = "Devices with a broken connection were found. Auto-fixes have been attempted where possible. Please review:"
				$DeviceTable = @($BrokenConnections) | ConvertTo-HTML -Fragment -As Table | Out-String

				$HTMLEmail = $EmailTemplate -f `
								$EmailIntro, 
								"Devices Found with Broken Connections", 
								$DeviceTable, 
								"<br />Please review and fix these devices manually where necessary."

				$mailbody = @{
					"From" = $EmailFrom
					"To" = $EmailTo_FailedFixes
					"Subject" = $EmailSubject
					"HTMLContent" = $HTMLEmail
				} | ConvertTo-Json -Depth 6

				$headers = @{
					'x-api-key' = $Email_APIKey.Key
				}

				Invoke-RestMethod -Method Post -Uri $Email_APIKey.Url -Body $mailbody -Headers $headers -ContentType application/json
				Write-Host "Email Sent." -ForegroundColor Yellow

				# log the sent email
				log_change -Company_Acronym $Company_Acronym -ServiceTarget 'email' -RMM_Device_ID "" -SC_Device_ID "" -Sophos_Device_ID "" -ChangeType $EmailChangeType -Hostname "" -Reason $EmailReason
			}
		}

		Write-Host "Broken connections check complete."
		Write-Host "======================"
	}

	# Find devices that are in one system but not another
	if ($DOMissingConnectionSearch) {
		Write-Host "Searching for devices missing a connection..."
		$MissingConnections = @()
		$SendEmail = $false
		$Now = Get-Date

		foreach ($Device in $MatchedDevices) {
			$SCDeviceIDs = @($Device.sc_matches)
			$RMMDeviceIDs = @($Device.rmm_matches)
			$SophosDeviceIDs = @($Device.sophos_matches)

			$ActivityComparison = $Device.activity_comparison
			$Activity = $ActivityComparison.Values | Sort-Object last_active
			$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
			$Timespan = New-TimeSpan -Start $NewestDate -End $Now

			if ($Timespan.Days -gt $InactiveDeleteDays) {
				# Inactive device, ignore (it'll get picked up in the next section)
				continue
			}
		
			if (($SCDeviceIDs | Measure-Object).count -eq 0 -or ($RMMDeviceIDs | Measure-Object).count -eq 0 -or ($SophosDeviceIDs | Measure-Object).count -eq 0) {
				$MissingTypes = @()

				if (($SCDeviceIDs | Measure-Object).count -eq 0 -and !(ignore_install -Device $Device -System 'sc')) {
					$MissingTypes += 'sc'
				}
				if (($RMMDeviceIDs | Measure-Object).count -eq 0 -and !(ignore_install -Device $Device -System 'rmm')) {
					$MissingTypes += 'rmm'
				}
				if (($SophosDeviceIDs | Measure-Object).count -eq 0 -and !(ignore_install -Device $Device -System 'sophos')) {
					$MissingTypes += 'sophos'
				}

				if ($MissingTypes -contains 'sc' -and $MissingTypes -contains 'rmm' -and $MissingTypes -notcontains 'sophos' -and $Timespan.Days -gt 7) {
					# If this device is only in Sophos and hasn't been seen in the past week, lets just skip it.
					# It likely was decomissioned and Sophos wasn't removed
					continue
				}

				if (($MissingTypes | Measure-Object).count -gt 0) {
					$Hostnames = @()
					$DeviceType = $false
					
					if ($SCDeviceIDs) {
						$SCDevices = @()
						foreach ($DeviceID in $SCDeviceIDs) {
							$SCDevices += $SC_DevicesHash[$DeviceID]
						}
						$Hostnames += $SCDevices.Name
						$DeviceType = $SCDevices[0].DeviceType
					}
					if ($RMMDeviceIDs) {
						$RMMDevices = @()
						foreach ($DeviceID in $RMMDeviceIDs) {
							$RMMDevices += $RMM_DevicesHash[$DeviceID]
						}
						$Hostnames += $RMMDevices."Device Hostname"
						if ($DeviceType) {
							$DeviceType = $RMMDevices[0]."Device Type"
						}
					}
					if ($SophosDeviceIDs) {
						$SophosDevices = @()
						foreach ($DeviceID in $SophosDeviceIDs) {
							$SophosDevices += $Sophos_DevicesHash[$DeviceID]
						}						
						$Hostnames += $SophosDevices.hostname
						if (!$DeviceType) {
							$DeviceType = $SophosDevices[0].type
						}
					}
					$Hostnames = $HostNames | Sort-Object -Unique

					$SCLink = ""
					$RMMLink = ""
					$SophosLink = ""
					if ($SCDeviceIDs) {
						$SCLink = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevices[0].Name)/$($SCDeviceIDs[0])"
					}
					if ($RMMDeviceIDs) {
						if ($RMMDevices[0].url) {
							$RMMLink = $RMMDevices[0].url
						} else {
							$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceIDs[0])"
						}
					}
					if ($SophosDeviceIDs) {
						$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevices[0].webID)"
					}
					$RMMAntivirus = $false
					if (!$SophosDeviceIDs -and $RMMDeviceIDs) {
						$RMMAntivirus = $RMMDevices[0].Antivirus
					}

					# See if we can try to automatically fix this issue
					$AutoFix = $false
					if (!$ReadOnly -and $MissingTypes -contains 'rmm' -and $MissingTypes -notcontains 'sc' -and $RMM_ID -and $SCLogin.Username -and $SCWebSession -and $Device.id -notin $MoveDevices.ID -and $Timespan.Days -lt $InactiveDeleteDaysRMM) {
						# Device missing in rmm but is in SC and we are using the SC api account, have the rmm org id, and this device is not in the $MoveDevices array (devices that look like they dont belong)
						# Only continue of the device was seen recently in SC (this will only work if it is active) and is using Windows
						foreach ($SCDevice in $SCDevices) {
							$LogParams = @{
								ServiceTarget = "sc"
								SC_Device_ID = $SCDevice.SessionID
								ChangeType = "install_rmm"
								Hostname = $SCDevice.Name
							}
							$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
							$EmailError = "RMM is not installed on $($LogParams.Hostname). The Device Audit script has tried to install RMM via SC $AttemptCount times now but it has not succeeded."
							$LogParams.RMM_Device_ID = $RMMDeviceIDs
							$LogParams.Sophos_Device_ID = $SophosDeviceIDs
							$LogParams.Reason = "RMM not installed"

							if ($SCDevice.GuestOperatingSystemName -like "*Windows*" -and $SCDevice.GuestOperatingSystemName -notlike "*Windows Embedded*") {
								if ($SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
									if (install_rmm_using_sc -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
										$AutoFix = $true
										check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
										log_change @LogParams -Company_Acronym $Company_Acronym
									}
								} else {
									add_device_to_install_queue -SC_ID $SCDevice.SessionID -ToInstall 'rmm'
								}
							} elseif ($SCDevice.GuestOperatingSystemName -like "*Mac OS*") {
								if ($SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
									if (install_rmm_using_sc_mac -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
										$AutoFix = $true
										check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
										log_change @LogParams -Company_Acronym $Company_Acronym
									}
								} else {
									add_device_to_install_queue -SC_ID $SCDevice.SessionID -ToInstall 'rmm'
								}
							}
						}
					}

					if (!$ReadOnly -and $MissingTypes -contains 'sc' -and $MissingTypes -notcontains 'rmm' -and $RMM_ID -and $Device.id -notin $MoveDevices.ID) {
						# Device missing in sc but is in RMM and we are using the rmm api, and this device is not in the $MoveDevices array (devices that look like they dont belong)
						# Only continue of the device was seen in RMM in the last 24 hours
						foreach ($RMMDevice in $RMMDevices) {
							if ($RMMDevice.suspended -ne "True" -and ($RMMDevice.Status -eq "Online" -or $RMMDevice."Last Seen" -eq "Currently Online" -or ($RMMDevice."Last Seen" -as [DateTime]) -gt (Get-Date).AddHours(-24))) {
								if (install_sc_using_rmm -RMM_Device $RMMDevice) {
									$LogParams = @{
										ServiceTarget = "rmm"
										RMM_Device_ID = $RMMDevice."Device UID"
										ChangeType = "install_sc"
										Hostname = $RMMDevice."Device Hostname"
									}
									$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
									$EmailError = "ScreenConnect is not installed on $($LogParams.Hostname). The Device Audit script has tried to install SC via RMM $AttemptCount times now but it has not succeeded."
									$LogParams.SC_Device_ID = $SCDeviceIDs
									$LogParams.Sophos_Device_ID = $SophosDeviceIDs
									$LogParams.Reason = "SC not installed"
	
									$AutoFix = $true
									check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
									log_change @LogParams -Company_Acronym $Company_Acronym
								}
							}
						}
					}

					$RepairTickets_Subset = @()
					$MissingTypes | ForEach-Object {
						if ($_ -in @("rmm", "sc", "sophos")) {
							$RepairTickets_Subset += repair_tickets -ServiceTarget $_ -Hostname $Hostnames
						}
					}					

					if (!$AutoFix -and $Device.id -notin $MoveDevices.ID -and $Timespan.Days -le 7 -and ($RepairTickets_Subset | Measure-Object).Count -eq 0) {
						$SendEmail = $true
					}

					$MissingConnections += [PsCustomObject]@{
						Hostnames = $Hostnames -join ', '
						DeviceType = $DeviceType
						LastActive = $NewestDate
						InSC = if ($SCDeviceIDs) { "Yes" } elseif (ignore_install -Device $Device -System 'sc') { "Ignore" } else { "No" }
						InRMM = if ($RMMDeviceIDs) { "Yes" }  elseif (ignore_install -Device $Device -System 'rmm') { "Ignore" } else { "No" }
						InSophos = if ($SophosDeviceIDs) { "Yes" } elseif ($RMMAntivirus -and $RMMAntivirus -like "Sophos*") { "Yes, missing from portal" } elseif (ignore_install -Device $Device -System 'sophos') { "Ignore" } else { "No" }
						AutoFix_Attempted = $AutoFix
						RepairTicket = if (($RepairTickets_Subset | Measure-Object).Count -gt 0) { $RepairTickets_Subset.ticketNumber -join ", " } else { "NA" }
						SC_Link = $SCLink
						RMM_Link = $RMMLink
						Sophos_Link = $SophosLink
					}
				}
			}
		}

		if ($SendEmail) {
			Write-Host "Warning! Devices were found that are missing in 1 or more systems (RMM, SC, or Sophos)." -ForegroundColor Red
			
			# Try sending an email
			$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
			$EmailChangeType = "missing-connections-email"
			$Reason = "Email sent for devices found with a missing connection"

			$EmailQuery_Params = @{
				LogHistory = $LogHistory
				StartTime = $TwoWeeksAgo
				EndTime = 'now'
				ServiceTarget = 'email'
				ChangeType = $EmailChangeType
				Reason = $EmailReason
			}
			$History_Subset_Emails = log_query @EmailQuery_Params

			# no emails were already sent, continue
			if (($History_Subset_Emails | Measure-Object).count -eq 0) {
				# send an email
				$EmailSubject = "Device Audit on $Company_Acronym - Devices Found that are missing a connection"
				$EmailIntro = "Devices were found that are missing in 1 or more systems (RMM, SC, or Sophos). Auto-fixes have been attempted where possible. Please review:"
				$DeviceTable = @($MissingConnections) | Where-Object { $_.LastActive -gt (Get-Date (Get-Date).AddDays(-7)) } | ConvertTo-HTML -Fragment -As Table | Out-String

				$HTMLEmail = $EmailTemplate -f `
								$EmailIntro, 
								"Devices Found with Missing Connections", 
								$DeviceTable, 
								"<br />Please review and fix these devices manually where necessary."

				$mailbody = @{
					"From" = $EmailFrom
					"To" = $EmailTo_FailedFixes
					"Subject" = $EmailSubject
					"HTMLContent" = $HTMLEmail
				} | ConvertTo-Json -Depth 6

				$headers = @{
					'x-api-key' = $Email_APIKey.Key
				}

				Invoke-RestMethod -Method Post -Uri $Email_APIKey.Url -Body $mailbody -Headers $headers -ContentType application/json
				Write-Host "Email Sent." -ForegroundColor Yellow

				# log the sent email
				log_change -Company_Acronym $Company_Acronym -ServiceTarget 'email' -RMM_Device_ID "" -SC_Device_ID "" -Sophos_Device_ID "" -ChangeType $EmailChangeType -Hostname "" -Reason $EmailReason
			}
		}

		Write-Host "Missing connections check complete."
		Write-Host "======================"
	}

	if (!(Test-Path variable:InactiveDeleteDaysRMM)) {
		$InactiveDeleteDaysRMM = $InactiveDeleteDays
	}

	# Check for devices that haven't been seen in a long time (in $InactiveDeleteDays) and suggest they be deleted
	if ($DOInactiveSearch) {
		Write-Host "Searching for old inactive devices..."
		$InactiveDevices = @()
		$Now = Get-Date
		foreach ($Device in $MatchedDevices) {
			$ActivityComparison = $Device.activity_comparison
			$Activity = $ActivityComparison.Values | Sort-Object last_active

			if (($Activity | Measure-Object).count -gt 0) {
				$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
				$Timespan = New-TimeSpan -Start $NewestDate -End $Now
				$DeviceIssueCheckRan = $true
				
				if ($Timespan.Days -gt $InactiveDeleteDays -or ($Activity.type -contains "rmm" -and $Timespan.Days -gt $InactiveDeleteDaysRMM)) {
					$RMMOnly = $false
					if ($Timespan.Days -lt $InactiveDeleteDays){
						$RMMOnly = $true
					}

					$Hostnames = @()
					$SCDeviceID = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].id } else { $false }
					$RMMDeviceID = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].id } else { $false }
					$SophosDeviceID = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].id } else { $false }
					$JCDeviceID = $false
					if ($JCConnected) {
						$JCDeviceID = if ($ActivityComparison.jc) { $ActivityComparison.jc[0].id } else { $false }
					}
					$ITG_IDs = $Device.itg_matches

					$User = $false
					$OperatingSystem = $false
					$Model = $false
					$WarrantyExpiry = ''

					if ($SCDeviceID) {
						$SCDevice = $SC_DevicesHash[$SCDeviceID]
						$Hostnames += $SCDevice.Name
						$User = $SCDevice.GuestLoggedOnUserName
						$OperatingSystem = $SCDevice.GuestOperatingSystemName
						$Model = $SCDevice.GuestMachineModel
					}
					if ($RMMDeviceID) {
						$RMMDevice = $RMM_DevicesHash[$RMMDeviceID]
						$Hostnames += $RMMDevice."Device Hostname"
						if (!$User) {
							$User = ($RMMDevice."Last User" -split '\\')[1]
						}
						if (!$OperatingSystem) {
							$OperatingSystem = $RMMDevice."Operating System"
						}
						if (!$Model) {
							$Model = $RMMDevice."Device Model"
						}
						$WarrantyExpiry = $RMMDevice."Warranty Expiry"
					}
					if ($SophosDeviceID) {
						$SophosDevice = $Sophos_DevicesHash[$SophosDeviceID]
						$Hostnames += $SophosDevice.hostname
						if (!$User) {
							$User = $SophosDevice.LastUser
						}
						if (!$OperatingSystem) {
							$OperatingSystem = $SophosDevice.OS
						}
					}
					if ($JCDeviceID) {
						$JCDevice = $JC_DevicesHash[$JCDeviceID]
						$Hostnames += if ($JCDevice.hostname) { $JCDevice.hostname -replace ".local", "" } else { $JCDevice.displayName }
						if (!$User) {
							if ($JC_Users) {
								$User = $JC_Users | Where-Object { $_.SystemID -eq $JCDevice.id } | Select-Object -Property Username -ExpandProperty Username
							}
						}
						if (!$OperatingSystem) {
							$OperatingSystem = "$($JCDevice.os) $($JCDevice.version)"
						}
					}
					$Hostnames = $HostNames | Sort-Object -Unique

					$SCLink = ''
					$RMMLink = ''
					$SophosLink = ''
					$JCLink = ''
					if ($SCDeviceID) {
						$SCLink = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevice.Name)/$($SCDeviceID)"
					}
					if ($RMMDeviceID) {
						if ($RMMDevice.url) {
							$RMMLink = $RMMDevice.url
						} else {
							$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceID)"
						}
					}
					if ($SophosDeviceID) {
						$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevice.webID)"
					}
					if ($JCDeviceID) {
						$JCLink = "https://console.jumpcloud.com/#/devices/$($JCDevice.id)/details"
					}
					$RMMAntivirus = $false
					if (!$SophosDeviceID -and $RMMDeviceID) {
						$RMMAntivirus = $RMMDevice.Antivirus
					}

					$DeleteSC = "No"
					if ($SCDeviceID -and !$RMMOnly) {
						$DeleteSC = "Yes, manually delete"
						$AllowDeletion = $true
						if ($DontAutoDelete -and ($DontAutoDelete.Hostnames -contains $SCDevice.Hostname -or $DontAutoDelete.SC -contains $SCDevice.Hostname -or $DontAutoDelete.SC -contains $SCDeviceID)) {
							$AllowDeletion = $false
						}
						if (!$ReadOnly -and $AllowDeletion) {
							$Deleted = delete_from_sc -SC_ID $SCDeviceID  -SCWebSession $SCWebSession
							if ($Deleted) {
								$DeleteSC = "Yes, auto attempted"
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "sc" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $SCDeviceID -Sophos_Device_ID $Device.sophos_matches -ChangeType "delete" -Hostname $SCDevice.Name -Reason "Inactive"
							}
						}
					}

					$DeleteRMM = "No"
					if ($RMMDeviceID) {
						$AllowDeletion = $true
						if ($DontAutoDelete -and ($DontAutoDelete.Hostnames -contains $RMMDevice."Device Hostname" -or $DontAutoDelete.RMM -contains $RMMDevice."Device Hostname" -or $DontAutoDelete.RMM -contains $RMMDeviceID)) {
							$AllowDeletion = $false
						}
						if (!$ReadOnly -and !$RMMDevice.ToDelete -and $AllowDeletion) {
							delete_from_rmm -RMM_Device_ID $RMMDeviceID						
							$DeleteRMM = "Yes, udf set for deletion"
							log_change -Company_Acronym $Company_Acronym -ServiceTarget "rmm" -RMM_Device_ID $RMMDeviceID -SC_Device_ID $Device.sc_matches -Sophos_Device_ID $Device.sophos_matches -ChangeType "delete" -Hostname $SCDevice.Name -Reason "Inactive"
						} elseif ($RMMDevice.ToDelete) {
							$DeleteRMM = "Pending deletion"
						} else {
							$DeleteRMM = "Yes, manually delete"
						}
					}

					$DeleteSophos = "No"
					if ($SophosDeviceID -and !$RMMOnly) {
						$DeleteSophos = "Yes, manually delete if decommed"
						$AllowDeletion = $true
						if ($DontAutoDelete -and ($DontAutoDelete.Hostnames -contains $SophosDevice.hostname -or $DontAutoDelete.Sophos -contains $SophosDevice.hostname -or $DontAutoDelete.Sophos -contains $SophosDeviceID)) {
							$AllowDeletion = $false
						}
						<# if ($InactiveAutoDeleteSophos -and !$ReadOnly -and $Timespan.Days -gt $InactiveAutoDeleteSophos -and $AllowDeletion) {
							$Deleted = delete_from_sophos -Sophos_Device_ID $SophosDeviceID -TenantApiHost $TenantApiHost -SophosHeader $SophosHeader
							if ($Deleted) {
								$DeleteSophos = "Yes, auto attempted"
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "sophos" -Sophos_Device_ID $SophosDevice.webID -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $Device.sc_matches -ChangeType "delete" -Hostname $SCDevice.Name -Reason "Inactive"
							}
						} #>
					}

					$DeleteJC = "No"
					if ($JCDeviceID -and !$RMMOnly) {
						$DeleteJC = "Yes, manually delete"
						$AllowDeletion = $true
						if ($DontAutoDelete -and (($JCDevice.hostname -and $DontAutoDelete.Hostnames -contains $JCDevice.hostname) -or $DontAutoDelete.Hostnames -contains $JCDevice.displayName -or ($JCDevice.hostname -and $DontAutoDelete.JC -contains $JCDevice.hostname) -or $DontAutoDelete.JC -contains $JCDevice.displayName -or $DontAutoDelete.JC -contains $OrderedDevice.id)) {
							$AllowDeletion = $false
						}
						if (!$ReadOnly -and $AllowDeletion) {
							$Deleted = delete_from_jc -JC_ID $JCDeviceID
							if ($Deleted) {
								$DeleteJC = "Yes, auto attempted"
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "jc" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $Device.sc_matches -Sophos_Device_ID $Device.sophos_matches -JC_Device_ID $JCDeviceID -ChangeType "delete" -Hostname if ($JCDevice.hostname) { $JCDevice.hostname } else { $JCDevice.displayName } -Reason "Inactive"
							}
						}
					}

					$DeleteITG = "No"
					if ($ITGConnected -and $ITG_IDs -and !$RMMOnly) {
						if (!$ReadOnly) {
							foreach ($ID in $ITG_IDs) {
								$Deleted = archive_itg -ITG_Device_ID $ID
								if ($Deleted) {
									$DeleteITG = "Yes"
									$ITG_Device = $ITG_DevicesHash[$ID]
									log_change -Company_Acronym $Company_Acronym -ServiceTarget "itg" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $Device.sc_matches -Sophos_Device_ID $Device.sophos_matches -ChangeType "delete" -Hostname $ITG_Device.attributes.name -Reason "Inactive"
								}
							}
						}
					}


					$InactiveDeviceInfo = [PsCustomObject]@{
						Hostnames = $Hostnames -join ', '
						LastActive = $NewestDate
						User = $User
						OS = $OperatingSystem
						Model = $Model
						WarrantyExpiry = $WarrantyExpiry
						InSC = if ($SCDeviceID -and !$RMMOnly) { "Yes" } elseif ($SCDeviceID) { "Yes, but don't delete yet" } else { "No" }
						InRMM = if ($RMMDeviceID) { "Yes" } else { "No" }
						InSophos = if ($SophosDeviceID -and !$RMMOnly) { "Yes" } elseif ($SophosDeviceID) { "Yes, but don't delete yet" } elseif ($RMMAntivirus -and $RMMAntivirus -like "Sophos*") { "Yes, missing from portal" } else { "No" }
						InJC = if ($JCDeviceID -and !$RMMOnly) { "Yes" } elseif ($JCDeviceID) { "Yes, but don't delete yet" } else { "No" }
						DeleteSC = $DeleteSC
						DeleteRMM = $DeleteRMM
						DeleteSophos = $DeleteSophos
						DeleteJC = $DeleteJC
						ArchiveITG = $DeleteITG
						SC_Time = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].last_active } else { "NA" }
						RMM_Time = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].last_active } else { "NA" }
						Sophos_Time = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].last_active } else { "NA" }
						JC_Time = if ($JCConnected -and $ActivityComparison.jc) { $ActivityComparison.jc[0].last_active } else { "NA" }
						SC_Link = $SCLink
						RMM_Link = $RMMLink
						Sophos_Link = $SophosLink
						JC_Link = $JCLink
					}

					if (!$JCConnected) {
						$InactiveDeviceInfo.PSObject.Properties.Remove('InJC');
						$InactiveDeviceInfo.PSObject.Properties.Remove('DeleteJC');
						$InactiveDeviceInfo.PSObject.Properties.Remove('JC_Time');
						$InactiveDeviceInfo.PSObject.Properties.Remove('JC_Link');
					}

					$InactiveDevices += $InactiveDeviceInfo
				}
			}
		}

		if (($InactiveDevices | Measure-Object).count -gt 0) {
			Write-Host "Warning! Old inactive devices were found!" -ForegroundColor Red
		}

		Write-Host "Inactive devices check complete."
		Write-Host "======================"
	}

	# Save each user and the computer(s) they are using into the Usage database (for user audits and documenting who uses each computer), then update users assigned to each computer
	if ($DOUsageDBSave) {
		# Connect to Account and DB
		$Account_Name = "stats-$($Company_Acronym.ToLower())"
		$Account = Get-CosmosDbAccount -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup
		if (!$Account) {
			try {
				New-CosmosDbAccount -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup -Location 'WestUS2' -Capability @('EnableServerless')
			} catch { 
				Write-Host "Account creation failed for $Company_Acronym. Exiting..." -ForegroundColor Red
				exit
			} 
			$Account = Get-CosmosDbAccount -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup
			
		}
		$PrimaryKey = Get-CosmosDbAccountMasterKey -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup

		$DB_Name = "DeviceUsage"
		$cosmosDbContext = New-CosmosDbContext -Account $Account_Name -Database $DB_Name -Key $PrimaryKey
	
		# Create new DB if one does not already exist
		try {
			Get-CosmosDbDatabase -Context $cosmosDbContext -Id $DB_Name | Out-Null
		} catch {
			if ($_.Exception.Response.StatusCode -eq "NotFound") {
				try {
					New-CosmosDbDatabase -Context $cosmosDbContext -Id $DB_Name | Out-Null
				} catch { 
					Write-Host "Database creation failed. Exiting..." -ForegroundColor Red
					exit
				}
			}
		}
	
		# Create collections for this customer if they don't already exist
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Users" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Users" -PartitionKey "type" | Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: Users"
		}
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Computers" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Computers" -PartitionKey "type"| Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: Computers"
		}
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Usage" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Usage" -PartitionKey "yearmonth" | Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: Usage"
		}
		
		Write-Host "Saving usage stats..."
	
		$Now = Get-Date
		$Now_UTC = Get-Date (Get-Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
	
		# Get all the computers and users first and query them in powershell to reduce db usage. This is MUCH cheaper than querying as we go!
		$Query = "SELECT * FROM Computers c"
		$ExistingComputers = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Computers" -Query $Query -PartitionKey 'computer'
		$Query = "SELECT * FROM Users u"
		$ExistingUsers = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -Query $Query -PartitionKey 'user'

		# We will also only add a usage entry if one has not already been added today
		$Year_Month = Get-Date -Format 'yyyy-MM'
		$Query = "SELECT * FROM Usage u WHERE u.UseDateTime >= '$(Get-Date -UFormat '+%Y-%m-%dT00:00:00.000Z')'"
		$ExistingUsageToday = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -Query $Query -PartitionKey $Year_Month
	
		foreach ($Device in $MatchedDevices) {
			$ActivityComparison = $Device.activity_comparison
			$Activity = $ActivityComparison.Values | Sort-Object last_active
	
			if (($Activity | Measure-Object).count -gt 0) {
				$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
				$Timespan = New-TimeSpan -Start $NewestDate -End $Now
				$DeviceUsageUpdateRan = $true
	
				if ($Timespan.TotalHours -gt 6) {
					# The device has not been seen in the last 6 hours, lets just skip it
					continue;
				}
	
				$LastActive = $NewestDate;
				$SCDevices = @()
				foreach ($DeviceID in $Device.sc_matches) {
					$SCDevices += $SC_DevicesHash[$DeviceID]
				}
	
				# If this device exists in SC, lets make sure it was also recently logged into (not just on)
				$SCLastActive = ($SCDevices | Sort-Object -Property GuestLastActivityTime | Select-Object -Last 1).GuestLastActivityTime
				if ($SCLastActive -and (New-TimeSpan -Start $SCLastActive -End $Now).TotalHours -gt 6) { 
					# online but not logged in, skip it
					continue;
				} elseif ($SCLastActive) {
					# replace $LastActive with the last active time
					$LastActive = $SCLastActive
				}
	
				$RMMDevices = @()
				foreach ($DeviceID in $Device.rmm_matches) {
					$RMMDevices += $RMM_DevicesHash[$DeviceID]
				}
	
				if (!$SCDevices -and !$RMMDevices) {
					# Lets ignore anything that's only in sophos, we can't match those securely enough for this
					continue;
				}
	
				# The device has been recently seen and logged into (can only tell if it was recently logged into if the device is in SC)
				$Hostname = $false
				$Username = $false
				$Domain = $false
				$DeviceType = $false
				$OperatingSystem = $false
				$Manufacturer = $false
				$Model = $false
				$WarrantyExpiry = $false
	
				if ($RMMDevices) {
					$RMMDevice = $RMMDevices | Sort-Object -Property "Last Seen" | Select-Object -Last 1
					$Hostname = $RMMDevice."Device Hostname"
					$DeviceType = $RMMDevice."Device Type"
					$OperatingSystem = $RMMDevice."Operating System"
					$Manufacturer = $RMMDevice."Manufacturer"
					$Model = $RMMDevice."Device Model"
					$WarrantyExpiry = $RMMDevice."Warranty Expiry"
	
					if ($RMMDevice."Last User" -like "*\*") {
						$Username = ($RMMDevice."Last User" -split '\\')[1]
					} else {
						$Username = $RMMDevice."Last User"
					}
					if ($RMMDevice."Last User" -like "AzureAD\*") {
						$Domain = "AzureAD"
					} else {
						$Domain = $RMMDevice.domain
					}
					if (!$Domain -or $Domain -like $Hostname) {
						$Domain = $false
					}
				}
	
				if ($SCDevices) {
					$SCDevice = $SCDevices | Sort-Object -Property GuestLastSeen | Select-Object -Last 1
					if (!$Hostname) {
						$Hostname = $SCDevice.Name
					}
					if (!$DeviceType) {
						$DeviceType = $SCDevice.DeviceType
					}
					if (!$Username) {
						$Username = $SCDevice.GuestLoggedOnUserName
					}
					if (!$Domain -and $SCDevice.GuestLoggedOnUserDomain -and $SCDevice.GuestLoggedOnUserDomain -notlike $Hostname) {
						$Domain = $SCDevice.GuestLoggedOnUserDomain
					}
					$OperatingSystem = $SCDevice.GuestOperatingSystemName
					if (!$Manufacturer) {
						$Manufacturer = $SCDevice.GuestMachineManufacturerName
					}
					if (!$Model) {
						$Model = $SCDevice.GuestMachineModel
					}
				}
	
				if (!$Username -or $Username -in $UsernameBlacklist) {
					# skip this if it's in the username blacklist
					continue;
				}
	
				$SerialNumbers = @()
				$SerialNumbers += $SCDevices.GuestMachineSerialNumber
				$SerialNumbers += $RMMDevices."Serial Number"
				$SerialNumbers = $SerialNumbers | Where-Object { $_ -notin $IgnoreSerials }
				$SerialNumbers = $SerialNumbers | Sort-Object -Unique
	
				if (!$DeviceType) {
					if ($OperatingSystem -and $OperatingSystem -like "*Server*") {
						$DeviceType = "Server"
					} else {
						$DeviceType = "Workstation"
					}
				}
				
				if ($DeviceType -eq "Server") {
					# Skip servers
					continue;
				}
	
				# cleanup data to be more readable
				if ($Manufacturer) {
					$Manufacturer = manufacturer_cleanup -Manufacturer $Manufacturer
				}
	
				if ($OperatingSystem) {
					if ($OperatingSystem -like "Microsoft*") {
						$OperatingSystem = $OperatingSystem -replace " ?(((\d+)\.*)+)$", ""
					} elseif ($OperatingSystem -like "VMware*") {
						$OperatingSystem = $OperatingSystem -replace " ?(build\d*) (((\d+)\.*)+)$", ""
					}
				}
	
				if ($WarrantyExpiry) {
					$WarrantyExpiry = $WarrantyExpiry -replace " UTC$", ""
					$WarrantyExpiry = ([DateTime]$WarrantyExpiry).ToString("yyyy-MM-ddT00:00:00.000Z")
				}
	
				$LastActive = Get-Date $LastActive.ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
				$Year_Month = Get-Date $LastActive -Format 'yyyy-MM'
	
				# Lets see if this computer is already in the database
				if ($SerialNumbers) {
					$Computers = $ExistingComputers | Where-Object { $_.SerialNumber -match "\|" + ($SerialNumbers -join "|") + "\|" }
				} else {
					$Computers = $ExistingComputers | Where-Object { $_.Hostname -like $Hostname }
				}
	
				# Narrow down to 1 computer
				if (($Computers | Measure-Object).Count -gt 1) {
					$Computers_Accuracy = $Computers;
					$Computers_Accuracy | Add-Member -NotePropertyName Accuracy -NotePropertyValue 0
					foreach ($Computer in $Computers_Accuracy) {
						$Accuracy = 0;
						if ($Hostname -like $Computer.Hostname) {
							$Accuracy++
						}
						$Device.sc_matches | ForEach-Object {
							if ("%|$($_)|%" -like $Computer.SC_ID) {
								$Accuracy++
							}
						}
						$Device.rmm_matches | ForEach-Object {
							if ("%|$($_)|%" -like $Computer.RMM_ID) {
								$Accuracy++
							}
						}
						$Device.sophos_matches | ForEach-Object {
							if ("%|$($_)|%" -like $Computer.Sophos_ID) {
								$Accuracy++
							}
						}
						$SerialNumbers | ForEach-Object {
							if ("%|$($_)|%" -like $Computer.SerialNumber) {
								$Accuracy++
							}
						}
						if ($Manufacturer -like $Computer.Manufacturer) {
							$Accuracy++
						}
						if ($Model -like $Computer.Model) {
							$Accuracy++
						}
						if ($OperatingSystem -like $Computer.OS) {
							$Accuracy++
						}
						$Computer.Accuracy = $Accuracy
					}
	
					$BestComputer = $Computers_Accuracy | Sort-Object -Property Accuracy, LastUpdated -Descending | Select-Object -First 1
					$Computers = $Computers | Where-Object { $_.id -eq $BestComputer[0].id }
				}
	
				$SC_String = "|$($Device.sc_matches -join '|')|"
				$RMM_String = "|$($Device.rmm_matches -join '|')|"
				$Sophos_String = "|$($Device.sophos_matches -join '|')|"
				$ITG_String = "|$($Device.itg_matches -join '|')|"
				$Autotask_String = "|$($Device.autotask_matches -join '|')|"
				$SerialNum_String = "|$($SerialNumbers  -join '|')|"
	
				if (!$Computers) {
					# No computer found, lets insert it
					$ComputerID = $([Guid]::NewGuid().ToString())
					$Computer = @{
						id = $ComputerID
						Hostname = $Hostname
						SC_ID = $SC_String
						RMM_ID = $RMM_String
						Sophos_ID = $Sophos_String
						Autotask_ID = $Autotask_String
						ITG_ID = $ITG_String
						DeviceType = $DeviceType
						SerialNumber = $SerialNum_String
						Manufacturer = $Manufacturer
						Model = $Model
						OS = $OperatingSystem
						WarrantyExpiry = $WarrantyExpiry
						LastUpdated = $Now_UTC
						type = "computer"
					} | ConvertTo-Json
					New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Computers" -DocumentBody $Computer -PartitionKey 'computer' | Out-Null
				} else {
					# Computer exists, see if we need to update the details
					$UpdateRequired = $false
					$UpdatedComputer = $Computers[0] | Select-Object Id, Hostname, SC_ID, RMM_ID, Sophos_ID, Autotask_ID, ITG_ID, DeviceType, SerialNumber, Manufacturer, Model, OS, WarrantyExpiry, LastUpdated, type
					if ($Hostname -ne $Computers.Hostname) {
						$UpdatedComputer.Hostname = $Hostname
						$UpdateRequired = $true
					}
					if ($DeviceType -ne $Computers.DeviceType) {
						$UpdatedComputer.DeviceType = $DeviceType
						$UpdateRequired = $true
					}
					if ($Manufacturer -ne $Computers.Manufacturer) {
						$UpdatedComputer.Manufacturer = $Manufacturer
						$UpdateRequired = $true
					}
					if ($Model -ne $Computers.Model) {
						$UpdatedComputer.Model = $Model
						$UpdateRequired = $true
					}
					if ($OperatingSystem -ne $Computers.OS) {
						$UpdatedComputer.OS = $OperatingSystem
						$UpdateRequired = $true
					}
					if ($WarrantyExpiry -ne $Computers.WarrantyExpiry) {
						$UpdatedComputer.WarrantyExpiry = $WarrantyExpiry
						$UpdateRequired = $true
					}
		
					if ($SC_String -ne $Computers.SC_ID) {
						$UpdatedComputer.SC_ID = $SC_String
						$UpdateRequired = $true
					}
					if ($RMM_String -ne $Computers.RMM_ID) {
						$UpdatedComputer.RMM_ID = $RMM_String
						$UpdateRequired = $true
					}
					if ($Sophos_String -ne $Computers.Sophos_ID) {
						$UpdatedComputer.Sophos_ID = $Sophos_String
						$UpdateRequired = $true
					}
					if (!$Computers.Autotask_ID -or $Autotask_String -ne $Computers.Autotask_ID) {
						if (!(Get-Member -inputobject $UpdatedComputer -name "Autotask_ID" -Membertype Properties)) {
							$UpdatedComputer | Add-Member -NotePropertyName Autotask_ID -NotePropertyValue $null
						}
						$UpdatedComputer.Autotask_ID = $Autotask_String
						$UpdateRequired = $true
					}
					if (!$Computers.ITG_ID -or $ITG_String -ne $Computers.ITG_ID) {
						if (!(Get-Member -inputobject $UpdatedComputer -name "ITG_ID" -Membertype Properties)) {
							$UpdatedComputer | Add-Member -NotePropertyName ITG_ID -NotePropertyValue $null
						}
						$UpdatedComputer.ITG_ID = $ITG_String
						$UpdateRequired = $true
					}
					if ($SerialNum_String -ne $Computers.SerialNumber) {
						$UpdatedComputer.SerialNumber = $SerialNum_String
						$UpdateRequired = $true
					}
		
					$ComputerID = $Computers[0].Id
					if ($UpdateRequired) {
						$UpdatedComputer.LastUpdated = $Now_UTC
						Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Computers" -Id $ComputerID -DocumentBody ($UpdatedComputer | ConvertTo-Json) -PartitionKey 'computer' | Out-Null
					}
				}

				if ($Domain -and $Domain -eq "AzureAD") { 
					$DomainOrLocal = "AzureAD"
				} elseif ($Domain -and $Domain -ne "WORKGROUP") {
					$DomainOrLocal = "Domain"
				} else { 
					$DomainOrLocal = "Local"
				}
	
				# Get the User ID, if not already in DB, add a new user
				$User = $ExistingUsers | Where-Object { $_.Username -like $Username }
	
				if (!$User) {
					# Add user
					$UserID = $([Guid]::NewGuid().ToString())
					$User = @{
						id = $UserID
						Username = $Username
						DomainOrLocal = $DomainOrLocal
						Domain = $Domain
						ADUsername = $null
						O365Email = $null
						ITG_ID = $null
						LastUpdated = $Now_UTC
						type = "user"
					} | ConvertTo-Json
					New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -DocumentBody $User -PartitionKey 'user' | Out-Null
				} else {
					# If changing fields, update in user audit as well
					$User = $User | Select-Object Id, Domain, DomainOrLocal, Username, LastUpdated, type, O365Email, ITG_ID, ADUsername
					$UserID = $User[0].Id
					if (!$User.DomainOrLocal -or $User.Domain -ne $Domain -or $User.DomainOrLocal -ne $DomainOrLocal) {
						$User.DomainOrLocal = $DomainOrLocal
						$User.Domain = $Domain
						$User.LastUpdated = $Now_UTC
						Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -Id $UserID -DocumentBody ($User | ConvertTo-Json) -PartitionKey 'user' | Out-Null
					}
				}

				# If a usage entry already exists today, skip adding another
				if (($ExistingUsageToday | Where-Object { $_.ComputerID -eq $Computers.id -and $_.UserID -eq $User.id } | Measure-Object).Count -gt 1) {
					continue
				}
	
				# Add a usage entry
				$ID = $([Guid]::NewGuid().ToString())
				$Usage = @{
					id = $ID
					ComputerID = $ComputerID
					UserID = $UserID
					UseDateTime = $LastActive
					yearmonth = $Year_Month
				} | ConvertTo-Json
				New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -DocumentBody $Usage -PartitionKey $Year_Month | Out-Null
			}
		}
	
		# Create collections for monthly stats if necessary
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Variables" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Variables" -PartitionKey "variable" | Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: Variables"
		}
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "ComputerUsage" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "ComputerUsage" -PartitionKey "id" | Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: ComputerUsage"
		}
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "UserUsage" | Out-Null
		} catch {
			try {
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "UserUsage" -PartitionKey "id" | Out-Null
			} catch {
				Write-Host "Table creation failed. Exiting..." -ForegroundColor Red
				exit
			}
			Write-Host "Created new table: UserUsage"
		}
	
		# Get the last time we updated the monthly stats
		$StatsLastUpdated = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId 'Variables' -Query "SELECT * FROM Variables AS v WHERE v.variable = 'StatsLastUpdated'" -PartitionKey 'StatsLastUpdated'
	
		# If we last updated the monthly stats sometime before the end of last month, lets update them now
		# (that means last month hasn't been updated yet), and then update the LastUpdated variable
		$LastMonth = (Get-Date).AddMonths(-1)
		$LastDay = [DateTime]::DaysInMonth($LastMonth.Year, $LastMonth.Month)
		$CheckDate = [DateTime]::new($LastMonth.Year, $LastMonth.Month, $LastDay, 23, 59, 59)
		$Updated_ComputerUsage = @()
		$Updated_UserUsage = @()
		if (!$StatsLastUpdated -or ($StatsLastUpdated -and (Get-Date $StatsLastUpdated.LastUpdated) -lt $CheckDate) -or $ForceMonthlyUsageRollup) {
			# Get all usage documents
			$Year_Month = Get-Date (Get-Date).AddMonths(-1) -Format 'yyyy-MM'
			$Usage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -Query "SELECT * FROM Usage AS u WHERE u.yearmonth = '$Year_Month'" -PartitionKey $Year_Month
	
			# Calculate monthly stats
			if ($Usage) {
				# Get all existing monthly stats
				$ComputerIDs = $Usage.ComputerID | Select-Object -Unique
				$Query = "SELECT * FROM ComputerUsage AS cu"
				$Existing_ComputerUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "ComputerUsage" -Query $Query -QueryEnableCrossPartition $true
				$Existing_ComputerUsage = $Existing_ComputerUsage | Where-Object { $_.Id -in $ComputerIDs }

				$UserIDs = $Usage.UserID | Select-Object -Unique
				$Query = "SELECT * FROM UserUsage AS uu"
				$Existing_UserUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "UserUsage" -Query $Query -QueryEnableCrossPartition $true
				$Existing_UserUsage = $Existing_UserUsage | Where-Object { $_.Id -in $UserIDs }
	
				# Group all usage stats from this past month by computer, user, and computer/user
				$Monthly_UsageByComputer = $Usage | Select-Object ComputerID, UserID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property ComputerID
				$Monthly_UsageByUser = $Usage | Select-Object UserID, ComputerID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property UserID
				$Monthly_UsageByComputerUser = $Usage | Select-Object ComputerID, UserID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property ComputerID, UserID
				$Monthly_OutOfDays = ($Usage | Select-Object @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
	
				# Build the monthly stats for each computer
				foreach ($ComputerID in $ComputerIDs) {
					$ExistingEntry = $Existing_ComputerUsage | Where-Object { $_.id -eq $ComputerID }
	
					if ($ExistingEntry) {
						$New_UsageHistory = $ExistingEntry | Select-Object id, DaysActive, LastActive, UsersUsedBy
					} else {
						$New_UsageHistory = @{
							id = $ComputerID
							DaysActive = @{
								Total = 0
								LastMonth = 0
								LastMonthPercent = 0
								History = @{}
								HistoryPercent = @{}
							}
							LastActive = $null
							UsersUsedBy = @()
						}
					}
	
					$MonthsUsage = $Monthly_UsageByComputer | Where-Object { $_.Name -eq $ComputerID }
					$DaysActive = ($MonthsUsage.Group | Select-Object Day | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
					$DaysActivePercent = [Math]::Round($DaysActive / $Monthly_OutOfDays * 100)
	
					$New_UsageHistory.DaysActive.LastMonth = $DaysActive
					$New_UsageHistory.DaysActive.LastMonthPercent = $DaysActivePercent
					if ($New_UsageHistory.DaysActive.History -is 'PSCustomObject') {
						if (!$New_UsageHistory.DaysActive.History.$Year_Month) {
							$New_UsageHistory.DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
						}
						if (!$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month) {
							$New_UsageHistory.DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
						}
						$New_UsageHistory.DaysActive.History.$Year_Month = $DaysActive
						$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
					} else {
						$New_UsageHistory.DaysActive.History[$Year_Month] = $DaysActive
						$New_UsageHistory.DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
					}
					$LastActive = ($MonthsUsage.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
					if (!$New_UsageHistory.LastActive -or $LastActive -gt $New_UsageHistory.LastActive) {
						$New_UsageHistory.LastActive = $LastActive
					}
					if ($ExistingEntry) {
						$New_UsageHistory.DaysActive.Total = (($ExistingEntry.DaysActive.History.PSObject.Properties.Value | Measure-Object -Sum).Sum)
					} else {
						$New_UsageHistory.DaysActive.Total += $DaysActive
					}
					$New_UsageHistory.UsersUsedBy | Foreach-Object {
						$_.DaysActive.LastMonth = 0
						$_.DaysActive.LastMonthPercent = 0
					}
	
					# Update the UsersUsedBy array with usage stats for this computer on a per-user basis
					$MonthsUsageByUser = $Monthly_UsageByComputerUser | Where-Object { $_.Name -like "*$ComputerID*" }
					$Existing_UsersUsedBy = [Collections.Generic.List[Object]]$New_UsageHistory.UsersUsedBy
					foreach ($User in $MonthsUsageByUser) {
						$UserID = $User.Group[0].UserID
						$DaysActive = ($User.Group | Select-Object Day | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
						$DaysActivePercent = [Math]::Round($DaysActive / $Monthly_OutOfDays * 100)
	
						$ExistingIndex = $Existing_UsersUsedBy.FindIndex( {$args[0].id -eq $UserID } )
						if ($ExistingIndex -ge 0) {
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.LastMonth = $DaysActive
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.LastMonthPercent = $DaysActivePercent
							if ($New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History -is 'PSCustomObject') {
								if (!$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History.$Year_Month) {
									$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
								}
								if (!$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month) {
									$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
								}
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History.$Year_Month = $DaysActive
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
							} else {
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History[$Year_Month] = $DaysActive
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
							}
							$LastActive = ($MonthsUsageByUser.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
							if (!$New_UsageHistory.UsersUsedBy[$ExistingIndex].LastActive -or $LastActive -gt $New_UsageHistory.UsersUsedBy[$ExistingIndex].LastActive) {
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].LastActive = $LastActive
							}
							if ($New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History) {
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.Total = (($New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History.PSObject.Properties.Value | Measure-Object -Sum).Sum)
							} else {
								$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.Total += $DaysActive
							}
						} else {
							$New_UsageHistory.UsersUsedBy += @{
								id = $UserID
								DaysActive = @{
									Total = $DaysActive
									LastMonth = $DaysActive
									LastMonthPercent = $DaysActivePercent
									History = @{
										$Year_Month = $DaysActive
									}
									HistoryPercent = @{
										$Year_Month = $DaysActivePercent
									}
								}
								LastActive = ($MonthsUsageByUser.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
							}
						}
					}
	
					$Updated_ComputerUsage += $New_UsageHistory
				}
	
				# Update the DB with the new monthly computer usage stats
				foreach ($Updated_Usage in $Updated_ComputerUsage) {
					if ($Updated_Usage.id -in $Existing_ComputerUsage.id) {
						# update
						Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "ComputerUsage" -Id $Updated_Usage.id -DocumentBody ($Updated_Usage | ConvertTo-Json -Depth 10) -PartitionKey $Updated_Usage.id | Out-Null
					} else {
						# new
						New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "ComputerUsage" -DocumentBody ($Updated_Usage | ConvertTo-Json -Depth 10) -PartitionKey $Updated_Usage.id | Out-Null
					}
				}
	
				# Build the monthly stats for each user
				foreach ($UserID in $UserIDs) {
					$ExistingEntry = $Existing_UserUsage | Where-Object { $_.id -eq $UserID }
	
					if ($ExistingEntry) {
						$New_UsageHistory = $ExistingEntry | Select-Object id, DaysActive, LastActive, ComputersUsed
					} else {
						$New_UsageHistory = @{
							id = $UserID
							DaysActive = @{
								Total = 0
								LastMonth = 0
								LastMonthPercent = 0
								History = @{}
								HistoryPercent = @{}
							}
							LastActive = $null
							ComputersUsed = @()
						}
					}
	
					$MonthsUsage = $Monthly_UsageByUser | Where-Object { $_.Name -eq $UserID }
					$DaysActive = ($MonthsUsage.Group | Select-Object Day | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
					$DaysActivePercent = [Math]::Round($DaysActive / $Monthly_OutOfDays * 100)
	
					$New_UsageHistory.DaysActive.LastMonth = $DaysActive
					$New_UsageHistory.DaysActive.LastMonthPercent = $DaysActivePercent
					if ($New_UsageHistory.DaysActive.History -is 'PSCustomObject') {
						if (!$New_UsageHistory.DaysActive.History.$Year_Month) {
							$New_UsageHistory.DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
						}
						if (!$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month) {
							$New_UsageHistory.DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
						}
						$New_UsageHistory.DaysActive.History.$Year_Month = $DaysActive
						$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
					} else {
						$New_UsageHistory.DaysActive.History[$Year_Month] = $DaysActive
						$New_UsageHistory.DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
					}
					$LastActive = ($MonthsUsage.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
					if (!$New_UsageHistory.LastActive -or $LastActive -gt $New_UsageHistory.LastActive) {
						$New_UsageHistory.LastActive = $LastActive
					}
					if ($ExistingEntry) {
						$New_UsageHistory.DaysActive.Total = (($ExistingEntry.DaysActive.History.PSObject.Properties.Value | Measure-Object -Sum).Sum)
					} else {
						$New_UsageHistory.DaysActive.Total += $DaysActive
					}
					$New_UsageHistory.ComputersUsed | Foreach-Object {
						$_.DaysActive.LastMonth = 0
						$_.DaysActive.LastMonthPercent = 0
					}
	
					# Update the ComputersUsed array with usage stats for this user on a per-computer basis
					$MonthsUsageByComputer = $Monthly_UsageByComputerUser | Where-Object { $_.Name -like "*$UserID*" }
					$Existing_ComputersUsed = [Collections.Generic.List[Object]]$New_UsageHistory.ComputersUsed
					foreach ($Computer in $MonthsUsageByComputer) {
						$ComputerID = $Computer.Group[0].ComputerID
						$DaysActive = ($Computer.Group | Select-Object Day | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
						$DaysActivePercent = [Math]::Round($DaysActive / $Monthly_OutOfDays * 100)
	
						$ExistingIndex = $Existing_ComputersUsed.FindIndex( {$args[0].id -eq $ComputerID } )
						if ($ExistingIndex -ge 0) {
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.LastMonth = $DaysActive
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.LastMonthPercent = $DaysActivePercent
							if ($New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History -is 'PSCustomObject') {
								if (!$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History.$Year_Month) {
									$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
								}
								if (!$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month) {
									$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
								}
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History.$Year_Month = $DaysActive
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
							} else {
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History[$Year_Month] = $DaysActive
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
							}
							$LastActive = ($MonthsUsageByComputer.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
							if (!$New_UsageHistory.ComputersUsed[$ExistingIndex].LastActive -or $LastActive -gt $New_UsageHistory.ComputersUsed[$ExistingIndex].LastActive) {
								$New_UsageHistory.ComputersUsed[$ExistingIndex].LastActive = $LastActive
							}
							if ($New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History) {
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.Total = (($New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History.PSObject.Properties.Value | Measure-Object -Sum).Sum)
							} else {
								$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.Total += $DaysActive
							}
						} else {
							$New_UsageHistory.ComputersUsed += @{
								id = $ComputerID
								DaysActive = @{
									Total = $DaysActive
									LastMonth = $DaysActive
									LastMonthPercent = $DaysActivePercent
									History = @{
										$Year_Month = $DaysActive
									}
									HistoryPercent = @{
										$Year_Month = $DaysActivePercent
									}
								}
								LastActive = ($MonthsUsageByComputer.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
							}
						}
					}
	
					$Updated_UserUsage += $New_UsageHistory
				}
	
				# Update the DB with the new monthly user usage stats
				foreach ($Updated_Usage in $Updated_UserUsage) {
					if ($Updated_Usage.id -in $Existing_UserUsage.id) {
						# update
						Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "UserUsage" -Id $Updated_Usage.id -DocumentBody ($Updated_Usage | ConvertTo-Json -Depth 10) -PartitionKey $Updated_Usage.id | Out-Null
					} else {
						# new
						New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "UserUsage" -DocumentBody ($Updated_Usage | ConvertTo-Json -Depth 10) -PartitionKey $Updated_Usage.id | Out-Null
					}
				}
	
				# Update the LastUpdated variable
				$StatsUpdated = @{
					variable = 'StatsLastUpdated'
					LastUpdated = Get-Date (Get-Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
				}
				if ($StatsLastUpdated) {
					# update
					$StatsUpdated.id = $StatsLastUpdated.id
					Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Variables" -Id $StatsLastUpdated.id -DocumentBody ($StatsUpdated | ConvertTo-Json) -PartitionKey 'StatsLastUpdated' | Out-Null
				} else {
					# new
					$StatsUpdated.id = $([Guid]::NewGuid().ToString())
					New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Variables" -DocumentBody ($StatsUpdated | ConvertTo-Json) -PartitionKey 'StatsLastUpdated' | Out-Null
				}

				$MonthlyStatsUpdated = $true
			}
		}

		#####
		## Update the documented users for each computer
		#####

		# Get the last time we updated the users
		$ComputerUsersLastUpdated = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId 'Variables' -Query "SELECT * FROM Variables AS v WHERE v.variable = 'ComputerUsersLastUpdated'" -PartitionKey 'ComputerUsersLastUpdated'

		# Update the users every 7 days or after the monthly stats have just been updated and then update the LastUpdated variable
		# If we are within the first 7 days of the month, don't check the last weeks usage (as we are only grabbing usage from the current month for that purpose)
		$CurrentDate = Get-Date 
		$SevenDaysAgo = ($CurrentDate).AddDays(-7) 
		if ($ComputerUsersLastUpdated -and $ComputerUsersLastUpdated.LastUpdated) {
			$LastUpdated = (Get-Date $ComputerUsersLastUpdated.LastUpdated)
		} else {
			$LastUpdated = $null
		}

		if (!$LastUpdated -or $MonthlyStatsUpdated -or ($LastUpdated -and $LastUpdated -lt $SevenDaysAgo -and $CurrentDate.Day -gt 7)) {
			$UpdateWeekly = $false
			if ($CurrentDate.Day -gt 7) {
				$UpdateWeekly = $true
				$Year_Month = Get-Date -Format 'yyyy-MM'
				$SevenDaysAgoUTC = Get-Date ($SevenDaysAgo).ToUniversalTime() -UFormat '+%Y-%m-%dT00:00:00.000Z'
				$LastWeeksUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -Query "SELECT * FROM Usage AS u WHERE u.UseDateTime >= '$($SevenDaysAgoUTC)' ORDER BY u.UseDateTime DESC" -PartitionKey $Year_Month
			}

			# Get existing device users from the logs (we have to query related items per-device so this saves us a LOT of api calls)
			$ExistingDeviceUsers = $false
			if ($DeviceUsersLocation) {
				$DeviceUsersPath = "$($DeviceUsersLocation)\$($Company_Acronym)_device_users.json"
				if (Test-Path $DeviceUsersPath) {
					$ExistingDeviceUsers = Get-Content -Path $DeviceUsersPath -Raw | ConvertFrom-Json
				}
			}

			# Update ITG_ID's for existing users (this mainly matters for customers that don't use the User Device Audit and local accounts)
			if (($ExistingUsers | Where-Object { !$_.ITG_ID } | Measure-Object).Count -gt 0) {
				$Now_UTC = Get-Date (Get-Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
				$ITG_Contacts = Get-ITGlueContacts -organization_id $ITG_ID -page_size 10000
				if ($ITG_Contacts.data) {
					$ITG_Contacts = $ITG_Contacts.data
				}

				$PossibleUsernames = @{}
				foreach ($ITGContact in $ITG_Contacts) {
					foreach ($Format in @($UsernameFormat)) {
						$PossibleUsername = $Format.replace("[first]", $ITGContact.attributes.'first-name').replace("[last]", $ITGContact.attributes.'last-name').replace("[firstInitial]", $ITGContact.attributes.'first-name'[0]).replace("[lastInitial]", $ITGContact.attributes.'last-name'[0])
						if (!$PossibleUsernames[$PossibleUsername]) {
							$PossibleUsernames[$PossibleUsername] = @()
						}
						$PossibleUsernames[$PossibleUsername] += $ITGContact.id
					}
				}

				for ($j = 0; $j -lt $ExistingUsers.Count; $j++) {
					$DBUser = $ExistingUsers[$j]

					if (!$DBUser.ITG_ID) {
						if (!$DBUser.Username) {
							continue
						}
						$UpdateDB = $false

						# Check first for a local username match in the ITG contact notes
						$ITGContact = $ITG_Contacts | Where-Object { $_.attributes.notes -match "(^|\s|>)(AD )?Username: ($([Regex]::Escape($DBUser.Username.Trim())))($|\s|<)" }
						if ($ITGContact) {
							$ExistingUsers[$j].ITG_ID = ($ITGContact | Select-Object -First 1).Id
							if (($ITGContact | Measure-Object).Count -eq 1) {
								$UpdateDB = $true
							}
						}

						# Check based on default username format
						if ($PossibleUsernames[$DBUser.Username] -and !$ExistingUsers[$j].ITG_ID) {
							$ExistingUsers[$j].ITG_ID = $PossibleUsernames[$DBUser.Username][0]
							if (($PossibleUsernames[$DBUser.Username] | Measure-Object).Count -eq 1) {
								$UpdateDB = $true
							}
						}

						if ($UpdateDB -and $ExistingUsers[$j].ITG_ID) {
							# Update database
							$UpdatedUser = $DBUser | Select-Object Id, Domain, DomainOrLocal, Username, LastUpdated, type, O365Email, ITG_ID, ADUsername
							$UpdatedUser.ITG_ID = $ExistingUsers[$j].ITG_ID
							$UpdatedUser.LastUpdated = $Now_UTC
							Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -Id $DBUser.Id -DocumentBody ($UpdatedUser | ConvertTo-Json) -PartitionKey 'user' | Out-Null
						}
					}
				}
			}

			# Update our data on all the users and computers (we need this to match id's to ITG)
			$Query = "SELECT * FROM Computers c"
			$ExistingComputers = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Computers" -Query $Query -PartitionKey 'computer'
			$Query = "SELECT * FROM Users u"
			$ExistingUsers = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -Query $Query -PartitionKey 'user'

			if (!$LastUpdated -and (!$MonthlyStatsUpdated -or !$Updated_ComputerUsage -or !$Updated_UserUsage)) {
				# If we have not updated the users before, and we didn't just update the monthly stats, grab the most recent monthly stats
				# Otherwise, we'll only update based on these at the end of the month just after updating the monthly stats
				$Year_Month = Get-Date (Get-Date).AddMonths(-1) -Format 'yyyy-MM'
				$Usage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -Query "SELECT * FROM Usage AS u WHERE u.yearmonth = '$Year_Month'" -PartitionKey $Year_Month
				if ($Usage) {
					# Get all existing monthly stats
					$ComputerIDs = $Usage.ComputerID | Select-Object -Unique
					$Query = "SELECT * FROM ComputerUsage AS cu WHERE cu.id IN ('$($ComputerIDs -join "', '")')"
					$ComputerUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "ComputerUsage" -Query $Query -QueryEnableCrossPartition $true
		
					$UserIDs = $Usage.UserID | Select-Object -Unique
					$Query = "SELECT * FROM UserUsage AS uu WHERE uu.id IN ('$($UserIDs -join "', '")')"
					$UserUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "UserUsage" -Query $Query -QueryEnableCrossPartition $true
				}
			} elseif ($Updated_ComputerUsage -and $Updated_UserUsage) {
				$ComputerUsage = $Updated_ComputerUsage
				$UserUsage = $Updated_UserUsage
			} else {
				$ComputerUsage = $false
				$UserUsage = $false
			}

			$DeviceUsers = @()
			$AssignedUsers = @()
			$FullUpdate = $false

			# Do a FULL update (based on last months monthly stats)
			if ($ComputerUsage -and $UserUsage) {
				$FullUpdate = $true
				$Year_Month = Get-Date (Get-Date).AddMonths(-1) -Format 'yyyy-MM'

				# Go through each device and find the primary user and any secondary users who've used the device > 25% of the time
				foreach ($Device in $ITG_Devices) {
					if ($Device.attributes.archived) {
						continue
					}
					if ($Device.attributes.'configuration-type-kind' -ne 'workstation' -and $Device.attributes.'configuration-type-kind' -ne 'laptop') {
						continue
					}

					$ExistingComputer = $ExistingComputers | Where-Object { $_.ITG_ID -like "*|$($Device.id)|*" }
					if (!$ExistingComputer) {
						$DeviceUsers += [pscustomobject]@{
							ITG_Computer = $Device.id
							PrimaryUser = $null
							SecondaryUsers = @()
							RecentUsers = @()
						}
						continue
					}

					$UsageStats = $ComputerUsage | Where-Object { $_.id -eq $ExistingComputer.id }
					$UsersUsedBy = $UsageStats.UsersUsedBy | Where-Object { $_.DaysActive.History.$Year_Month } | Sort-Object -Property {$_.DaysActive.LastMonthPercent} -Descending
					$PrimaryUser = $UsersUsedBy | Select-Object -First 1
					$RemainingUsers = $UsersUsedBy | Select-Object -Skip 1
					$SecondaryUsers = @($RemainingUsers | Where-Object { $_.DaysActive.LastMonthPercent -ge 25 -and $_.DaysActive.LastMonth -ge 5 })

					$PrimaryUser_ITG = $null
					$SecondaryUsers_ITG = @()

					if ($PrimaryUser) {
						$AssignedUsers += $PrimaryUser.id
						$DBUser = $ExistingUsers | Where-Object { $_.id -eq $PrimaryUser.id }
						if ($DBUser -and $DBUser.ITG_ID) {
							$PrimaryUser_ITG = $DBUser.ITG_ID
						}
					}
					if ($SecondaryUsers) {
						foreach ($User in $SecondaryUsers) {
							$AssignedUsers += $User.id
							$DBUser = $ExistingUsers | Where-Object { $_.id -eq $User.id }
							if ($DBUser -and $DBUser.ITG_ID) {
								$SecondaryUsers_ITG += $DBUser.ITG_ID
							}
						}
					}

					$DeviceUsers += [pscustomobject]@{
						ITG_Computer = $Device.id
						PrimaryUser = $PrimaryUser_ITG
						SecondaryUsers = $SecondaryUsers_ITG
						RecentUsers = @()
					}
				}
				
				# Go through any users who have not been assigned to a computer, and assign them to whichever computer they have used the most in the past month
				foreach ($UserUse in $UserUsage) {
					if ($UserUse.id -in $AssignedUsers) {
						continue
					}
					if (!$UserUse.DaysActive.History.$Year_Month) {
						continue
					}

					$DBUser = $ExistingUsers | Where-Object { $_.id -eq $UserUse.id }
					if (!$DBUser.ITG_ID) {
						continue
					}

					$ComputersUsed = $UserUse.ComputersUsed | Where-Object { $_.DaysActive.History.$Year_Month } | Sort-Object -Property {$_.DaysActive.LastMonthPercent} -Descending
					$PrimaryComputer = $ComputersUsed | Select-Object -First 1

					$PrimaryComputer_ITG = $null
					if ($PrimaryComputer) {
						$AssignedUsers += $UserUse.id
						$DBComputer = $ExistingComputers | Where-Object { $_.id -eq $PrimaryComputer.id }
						if ($DBComputer -and $DBComputer.ITG_ID) {
							$Computer_ITG_IDs = @($DBComputer.ITG_ID.Split("|") | Where-Object { $_ })
							foreach ($ID in $Computer_ITG_IDs) {
								$DeviceUser = $DeviceUsers | Where-Object { $_.ITG_Computer -eq $ID }
								
								if ($DeviceUser) {
									# update
									if ($DeviceUser.PrimaryUser) {
										$DeviceUser.SecondaryUsers += $DBUser.ITG_ID
									} else {
										$DeviceUser.PrimaryUser = $DBUser.ITG_ID
									}
								} else {
									# new
									$DeviceUsers += [pscustomobject]@{
										ITG_Computer = $ID
										PrimaryUser = $DBUser.ITG_ID
										SecondaryUsers = @()
										RecentUsers = @()
									}
								}
							}
						}
					}
				}
			}

			# Do a WEEKLY update (based on usage from the last 7 days)
			# We will only add any heavy users from the last week, we won't remove any users
			if ($UpdateWeekly) {
				$ComputerIDs = $LastWeeksUsage.ComputerID | Select-Object -Unique
				$Monthly_UsageByComputerUser = $LastWeeksUsage | Select-Object ComputerID, UserID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property ComputerID, UserID
				$Monthly_OutOfDays = ($LastWeeksUsage | Select-Object @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count

				foreach ($ComputerID in $ComputerIDs) {
					$MonthsUsageByUser = $Monthly_UsageByComputerUser | Where-Object { $_.Name -like "*$ComputerID*" }
					$DBComputer = $ExistingComputers | Where-Object { $_.id -eq $ComputerID }

					if ($DBComputer -and $DBComputer.ITG_ID) {
						$Computer_ITG_IDs = @($DBComputer.ITG_ID.Split("|") | Where-Object { $_ })
					} else {
						continue
					}

					foreach ($User in $MonthsUsageByUser) {
						$UserID = $User.Group[0].UserID
						$DBUser = $ExistingUsers | Where-Object { $_.id -eq $UserID }
						$DaysActive = ($User.Group | Select-Object Day | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count
						$DaysActivePercent = [Math]::Round($DaysActive / $Monthly_OutOfDays * 100)

						if ($DBUser.ITG_ID -and $DaysActivePercent -ge 25) {
							# Usage in last week is high, add to computer if necessary
							foreach ($Device_ITG_ID in $Computer_ITG_IDs) {
								$DeviceUser = $DeviceUsers | Where-Object { $_.ITG_Computer -eq $Device_ITG_ID }

								if ($DeviceUser) {
									if ($DeviceUser.PrimaryUser -eq $DBUser.ITG_ID -or $DeviceUser.SecondaryUsers -contains $DBUser.ITG_ID -or $DeviceUser.RecentUsers -contains $DBUser.ITG_ID) {
										# already assigned to that computer
										continue
									} else {
										# update
										$DeviceUser.RecentUsers += $DBUser.ITG_ID
									}
								} else {
									# new
									$DeviceUsers += [pscustomobject]@{
										ITG_Computer = $Device_ITG_ID
										PrimaryUser = $null
										SecondaryUsers = @()
										RecentUsers = @($DBUser.ITG_ID)
									}
								}
								$DeviceUsersUpdateRan = $true
							}
						}
					}
				}
			}

			# All devices / users mapped, update IT Glue (if it's a full update, delete existing mappings as well)
			if ($DeviceUsers) {
				if (!$ITG_Contacts) {
					$ITG_Contacts = Get-ITGlueContacts -organization_id $ITG_ID -page_size 10000
					if ($ITG_Contacts.data) {
						$ITG_Contacts = $ITG_Contacts.data
					}
				}
				$ITGLocations = Get-ITGlueLocations -org_id $ITG_ID
				if ($ITGLocations.data) {
					$ITGLocations = $ITGLocations.data
				}
				$AutotaskContacts = Get-AutotaskAPIResource -Resource Contacts -SimpleSearch "companyID eq $Autotask_ID"
				$AutotaskContacts = $AutotaskContacts | Where-Object { $_.isActive }

				foreach ($Device in $DeviceUsers) {
					$ITG_Device = $ITG_DevicesHash[$Device.ITG_Computer]
					$MatchedDevice = $MatchedDevices | Where-Object { $_.itg_matches -contains $Device.ITG_Computer }
					$Autotask_Device = $false
					if ($MatchedDevice.autotask_matches) {
						$Autotask_Device = @()
						foreach ($DeviceID in $MatchedDevice.autotask_matches) {
							$Autotask_Device += $Autotask_DevicesHash[$DeviceID]
						}
					}
					$Existing_RelatedItems = $false
					$Existing_DeviceUsersLogged = $ExistingDeviceUsers | Where-Object { $_.ITG_Computer -eq $Device.ITG_Computer }

					# If the contact is synced with autotask then we cannot update ITG directly, we need to find the autotask contact and update autotask instead
					if ($Device.PrimaryUser -and $ITG_Contacts) {
						$ITG_Contact = $ITG_Contacts | Where-Object { $_.id -eq $Device.PrimaryUser }
						if ($ITG_Contact -and $ITG_Contact.attributes.'psa-integration' -eq 'enabled') {
							$PrimaryITGEmail = $ITG_Contact.attributes.'contact-emails' | Where-Object { $_.primary -eq "True" }
							$Autotask_Contact = $AutotaskContacts | Where-Object { $_.firstName -eq $ITG_Contact.attributes.'first-name' -and $_.lastName -eq $ITG_Contact.attributes.'last-name' -and $_.emailAddress -eq $PrimaryITGEmail.value }

							if (($Autotask_Contact | Measure-Object).Count -gt 1) {
								$Autotask_Contact = $Autotask_Contact | Where-Object { $_.title.Trim() -eq ($ITG_Contact.attributes.title | Out-String).Trim() -and ((($_.mobilePhone -replace '\D', '') -in $ITG_Contact.attributes.'contact-phones'.value -and ($_.phone -replace '\D', '') -in $ITG_Contact.attributes.'contact-phones'.value) -or !$ITG_Contact.attributes.'contact-phones'.value ) }
							}

							if (($Autotask_Contact | Measure-Object).Count -gt 1 -and $ITGLocations -and $ITG_Contact.attributes.'location-id') {
								$Location = $ITGLocations | Where-Object { $_.id -eq $ITG_Contact.attributes.'location-id' }
								$Autotask_Contact = $Autotask_Contact | Where-Object { $_.city -like $Location.attributes.city -and $_.state -like $Location.attributes.'region-name' -and ($_.zipCode -replace '\W', '') -like ($Location.attributes.'postal-code' -replace '\W', '') }
								if (($Autotask_Contact | Measure-Object).Count -gt 1) {
									$Autotask_Contact = $Autotask_Contact | Where-Object { $_.addressLine -like $Location.attributes.'address-1' }
								}
							}

							if (($Autotask_Contact | Measure-Object).Count -gt 1) {
								$Autotask_Contact = $Autotask_Contact[0]
							}
						}
					}

					$PSAIntegration = $false
					if ($ITG_Device -and $ITG_Device.attributes.'psa-integration' -and $ITG_Device.attributes.'psa-integration' -ne 'disabled') {
						$PSAIntegration = $true
					}

					if ($FullUpdate) {
						$ITGDetails = Get-ITGlueConfigurations -id $Device.ITG_Computer -include 'related_items'
						if ($ITGDetails.included) {
							$Existing_RelatedItems = $ITGDetails.included
						}

						# Remove any existing related items that don't match the new data
						if ($Existing_RelatedItems) {
							$AutoAssigned = $Existing_RelatedItems | Where-Object { $_.attributes.'asset-type' -eq 'contact' -and $_.attributes.notes -like "*(Auto-Assigned)" }
							$RemoveRelated = @()
							foreach ($RelatedItem in $AutoAssigned) {
								if ($RelatedItem.attributes.notes -like "Primary User*" -and $RelatedItem.attributes.'resource-id' -ne $Device.PrimaryUser) {
									$RemoveRelated += @{
										'type' = 'related_items'
										'attributes' = @{
											'id' = $RelatedItem.id
										}
									}
									$Existing_RelatedItems = $Existing_RelatedItems | Where-Object { $_.id -ne $RelatedItem.id }
								}
								if ($RelatedItem.attributes.notes -like "Secondary User*" -and $RelatedItem.attributes.'resource-id' -notin $Device.SecondaryUsers) {
									$RemoveRelated += @{
										'type' = 'related_items'
										'attributes' = @{
											'id' = $RelatedItem.id
										}
									}
									$Existing_RelatedItems = $Existing_RelatedItems | Where-Object { $_.id -ne $RelatedItem.id }
								}
								if ($RelatedItem.attributes.notes -like "Recently Seen*" -and $RelatedItem.attributes.'resource-id' -notin $Device.RecentUsers) {
									$RemoveRelated += @{
										'type' = 'related_items'
										'attributes' = @{
											'id' = $RelatedItem.id
										}
									}
									$Existing_RelatedItems = $Existing_RelatedItems | Where-Object { $_.id -ne $RelatedItem.id }
								}
							}

							if ($RemoveRelated) {
								Remove-ITGlueRelatedItems -resource_type 'configurations' -resource_id $Device.ITG_Computer -data $RemoveRelated
							}
						}

						# If no primary contact anymore, remove from configuration
						if (!$Device.PrimaryUser -and $ITG_Device.attributes.'contact-id') {
							if (!$PSAIntegration) {
								$UpdatedConfig = @{
									'type' = 'configurations'
									'attributes' = @{
										'contact-id' = ""
										'contact-name' = ""
									}
								}
								Set-ITGlueConfigurations -id $Device.ITG_Computer -data $UpdatedConfig
							}
							if ($Autotask_Contact -and $Autotask_Device) {

								foreach ($AutoDevice in $Autotask_Device) {
									$ConfigurationUpdate = 
									[PSCustomObject]@{
										contactID = ""
									}

									Set-AutotaskAPIResource -Resource ConfigurationItems -ID $AutoDevice.id -body $ConfigurationUpdate
								}
							}
						}
					}

					# Set primary contact
					if ($Device.PrimaryUser -and (!$ITG_Device -or $ITG_Device.attributes.'contact-id' -ne $Device.PrimaryUser)) {
						if (!$PSAIntegration) {
							$UpdatedConfig = @{
								'type' = 'configurations'
								'attributes' = @{
									'contact-id' = $Device.PrimaryUser
								}
							}
							Set-ITGlueConfigurations -id $Device.ITG_Computer -data $UpdatedConfig
						}
						if ($Autotask_Contact -and $Autotask_Device) {
							foreach ($AutoDevice in $Autotask_Device) {
								$ConfigurationUpdate = 
								[PSCustomObject]@{
									contactID = $Autotask_Contact.id
								}

								Set-AutotaskAPIResource -Resource ConfigurationItems -ID $AutoDevice.id -body $ConfigurationUpdate
							}
						}
					}

					# Add new related items, checking first if they already exist in $Existing_RelatedItems
					$RelatedItems = @()
					if ($Device.PrimaryUser) {
						if (!$Existing_RelatedItems -or ($Existing_RelatedItems | Where-Object { $_.attributes.'resource-id' -eq $Device.PrimaryUser -and $_.attributes.notes -like "*(Auto-Assigned)" } | Measure-Object).Count -eq 0) {
							$RelatedItems += @{
								type = 'related_items'
								attributes = @{
									destination_id = $Device.PrimaryUser
									destination_type = "Contact"
									notes = "Primary User (Auto-Assigned)"
								}
							}
						}
					}

					if ($Device.SecondaryUsers) {
						foreach ($User in $Device.SecondaryUsers) {
							if (!$Existing_RelatedItems -or ($Existing_RelatedItems | Where-Object { $_.attributes.'resource-id' -eq $User -and $_.attributes.notes -like "*(Auto-Assigned)" } | Measure-Object).Count -eq 0) {
								$RelatedItems += @{
									type = 'related_items'
									attributes = @{
										destination_id = $User
										destination_type = "Contact"
										notes = "Secondary User (Auto-Assigned)"
									}
								}
							}
						}
					}

					if ($Device.RecentUsers) {
						foreach ($User in $Device.RecentUsers) {
							if ((!$Existing_RelatedItems -or ($Existing_RelatedItems | Where-Object { $_.attributes.'resource-id' -eq $User -and $_.attributes.notes -like "*(Auto-Assigned)" } | Measure-Object).Count -eq 0) -and (!$Existing_DeviceUsersLogged -or ($Existing_DeviceUsersLogged.PrimaryUser -ne $User -and $Existing_DeviceUsersLogged.RecentUsers -notcontains $User -and $Existing_DeviceUsersLogged.SecondaryUsers -notcontains $User))) {
								$RelatedItems += @{
									type = 'related_items'
									attributes = @{
										destination_id = $User
										destination_type = "Contact"
										notes = "Recently Seen User (Auto-Assigned)"
									}
								}
							}
						}
					}

					if ($RelatedItems) {
						New-ITGlueRelatedItems -resource_type configurations -resource_id $Device.ITG_Computer -data $RelatedItems
					}
				}

				# Update "Computer Users Last Updated" variable to current datetime
				$StatsUpdated = @{
					variable = 'ComputerUsersLastUpdated'
					LastUpdated = Get-Date (Get-Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
				}
				if ($ComputerUsersLastUpdated) {
					# update
					$StatsUpdated.id = $ComputerUsersLastUpdated.id
					Set-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Variables" -Id $ComputerUsersLastUpdated.id -DocumentBody ($StatsUpdated | ConvertTo-Json) -PartitionKey 'ComputerUsersLastUpdated' | Out-Null
				} else {
					# new
					$StatsUpdated.id = $([Guid]::NewGuid().ToString())
					New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Variables" -DocumentBody ($StatsUpdated | ConvertTo-Json) -PartitionKey 'ComputerUsersLastUpdated' | Out-Null
				}
				$LastUpdated = Get-Date
				$DeviceUsersUpdateRan = $true
			}

			# Export an updated json file of device users
			if ($DeviceUsers -and $DeviceUsersPath) {
				if ($ExistingDeviceUsers -and !$FullUpdate) {
					$DeviceUsersExport = $ExistingDeviceUsers
					foreach ($Device in $DeviceUsers) {
						$DeviceUserExport = $DeviceUsersExport | Where-Object { $_.ITG_Computer -eq $Device.ITG_Computer }
						if (!$DeviceUserExport) {
							$DeviceUsersExport += $Device
						} else {
							if ($Device.PrimaryUser -and $Device.PrimaryUser -ne $DeviceUserExport.PrimaryUser) {
								$DeviceUserExport.PrimaryUser = $Device.PrimaryUser
							}
							if ($Device.SecondaryUsers) {
								foreach ($User in $Device.SecondaryUsers) {
									if ($User -notin $DeviceUserExport.SecondaryUsers) {
										$DeviceUserExport.SecondaryUsers += $User
									}
								}
							}
							if ($Device.RecentUsers) {
								foreach ($User in $Device.RecentUsers) {
									if ($User -notin $DeviceUserExport.RecentUsers -and $User -notin $DeviceUserExport.SecondaryUsers -and $User -ne $DeviceUserExport.PrimaryUser) {
										$DeviceUserExport.RecentUsers += $User
									}
								}
							}
						}
					}
				} else {
					$DeviceUsersExport = $DeviceUsers
				}
				$DeviceUsersExport | ConvertTo-Json | Out-File -FilePath $DeviceUsersPath
			}
		}

	
		Write-Host "Usage Stats Saved!"
		Write-Host "===================="
	}

	# Update device locations in Autotask/IT Glue
	if ($DOUpdateDeviceLocations -and $ITGConnected -and $ITG_ID) {
		Write-Host "Updating device locations..."
		$WANs = Get-ITGlueFlexibleAssets -page_size 1000 -filter_flexible_asset_type_id $WANFilterID.id -filter_organization_id $ITG_ID
		$LANs = Get-ITGlueFlexibleAssets -page_size 1000 -filter_flexible_asset_type_id $LANFilterID.id -filter_organization_id $ITG_ID
		if (!$WANs -or $WANs.Error) {
			Write-PSFMessage -Level Error -Message "An error occurred trying to get the existing WAN assets from ITG. Exiting..."
			Write-PSFMessage -Level Error -Message $WANs.Error
			$WANs = @()
		}
		if (!$LANs -or $LANs.Error) {
			Write-PSFMessage -Level Error -Message "An error occurred trying to get the existing LAN assets from ITG. Exiting..."
			Write-PSFMessage -Level Error -Message $LANs.Error
			$LANs = @()
		}
		if (!$ITGLocations) {
			$ITGLocations = Get-ITGlueLocations -org_id $ITG_ID
			if (!$ITGLocations -or $ITGLocations.Error) {
				Write-PSFMessage -Level Error -Message "An error occurred trying to get the existing location assets from ITG. Exiting..."
				Write-PSFMessage -Level Error -Message $ITGLocations.Error
				$ITGLocations = @()
			} else {
				$ITGLocations = $ITGLocations.data
			}
		}

		$UpdateOverview = $false
		if ($OverviewFilterID) {
			$UpdateOverview = $true
			$CustomOverviews = Get-ITGlueFlexibleAssets -page_size 1000 -filter_flexible_asset_type_id $OverviewFilterID.id -filter_organization_id $ITG_ID
			$i = 1
			while ($CustomOverviews.links.next) {
				$i++
				$CustomOverviews_Next = Get-ITGlueFlexibleAssets -page_size 1000 -page_number $i -filter_flexible_asset_type_id $OverviewFilterID.id -filter_organization_id $ITG_ID
				if (!$CustomOverviews_Next -or $CustomOverviews_Next.Error) {
					# We got an error querying configurations, wait and try again
					Start-Sleep -Seconds 2
					$CustomOverviews_Next = Get-ITGlueFlexibleAssets -page_size 1000 -page_number $i -filter_flexible_asset_type_id $OverviewFilterID.id -filter_organization_id $ITG_ID
			
					if (!$CustomOverviews_Next -or $CustomOverviews_Next.Error) {
						Write-PSFMessage -Level Error -Message "An error occurred trying to get the existing custom overviews from ITG. Exiting..."
						Write-PSFMessage -Level Error -Message $CustomOverviews_Next.Error
						$UpdateOverview = $false
						break
					}
				}
				$CustomOverviews.data += $CustomOverviews_Next.data
				$CustomOverviews.links = $CustomOverviews_Next.links
			}
			
			if ($CustomOverviews -and $CustomOverviews.data) {
				$WANCustomOverviews = $CustomOverviews.data | Where-Object { $_.attributes.name -like "WAN: *" }
				$LANCustomOverviews = $CustomOverviews.data | Where-Object { $_.attributes.name -like "LAN: *" }
			} else {
				$UpdateOverview = $false
			}
		}
		$IPRegex = "\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(-(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)?)?(\/[1-3][0-9])?\b"

		if ($LANs -and $LANs.data) {
			$LANs = $LANs.data
		}

		if ($WANs -and $WANs.data -and ($WANs.data | Measure-Object).Count -gt 0 -and $ITG_Devices -and $ITGLocations) {
			$WANs = $WANs.data

			$LocationIPs = @()
			$LANIPs = @{}

			foreach ($Location in $ITGLocations) {
				$LocationLANs = $LANs | Where-Object { $_.attributes.traits.'location-s'.values.id -contains $Location.id }
				$LocationWANs = @()
				$LocationWANs += $WANs | Where-Object { $_.attributes.traits.'location-s'.values.id -contains $Location.id }
				if ($LocationLANs) {
					$LocationWANs += $WANs | Where-Object { $_.id -in $LocationLANs.attributes.traits.'internet-wan'.values.id }	
				}
				$LocationWANs = $LocationWANs | Sort-Object | Get-Unique -AsString

				if (!$LocationWANs) {
					continue
				}

				$IPs_Parsed = @() # All of locations IPs
				$IPs_To_WAN = @{}
				foreach ($WAN in $LocationWANs) {
					$IPAddressInfo = $WAN.attributes.traits.'ip-address-es'
					$IPAddressInfo = $IPAddressInfo -replace [char]0x00a0,' '
					$IPHTML = ""
		
					# Parse the html on the WAN page
					if ($IPAddressInfo -like "*<table>*") {
						$HTML = New-Object -Com "HTMLFile"
						try {
							# This works in PowerShell with Office installed
							$HTML.IHTMLDocument2_write($IPAddressInfo)
						} catch {
							# This works when Office is not installed    
							$src = [System.Text.Encoding]::Unicode.GetBytes($IPAddressInfo)
							$HTML.write($src)
						}
		
						$TableData = $HTML.all | Where-Object { $_.tagname -eq 'td' }
						$TableHeaders = $TableData | Where-Object { $_.innerHtml -like "*<strong>*</strong>*" }
						$TableData = $TableData | Where-Object { $_.innerHtml -notlike "*<strong>*</strong>*" }
		
						$ColCount = $TableHeaders.Count
						if (!$ColCount) {
							$ColCount = $TableData.Count
						}
						for ($i = 0; $i -le $TableData.count; $i++) {
							$Column = $i % $ColCount
							if ($TableHeaders -and $TableHeaders[$Column]) {
								$Header = $TableHeaders[$Column].innerHTML
								if ($Header -like "*DNS*" -or $Header -like "*Subnet*") {
									continue
								}
							}
							$IPHTML += "`n$($TableData[$i].innerHTML)"
						}
					} elseif ($IPAddressInfo -like "*:*") {
						
						if ($IPAddressInfo -match "(^|>)(External )?IP(<.*>)?:?(<.*>)? *$IPRegex *?($|<)" -and $IPAddressInfo -match "Subnet ?(Mask)?(<.*>)?:?(<.*>)? *($IPRegex)") {
							$SubnetMask = $Matches[4]
							if ($SubnetMask) {
								$CidrRange = Convert-SubnetMaskToCidr $SubnetMask
								if ($CidrRange) {
									$IPAddressInfo -match "(^|>)(External )?IP(<.*>)?:?(<.*>)? *$IPRegex *?($|<)"
									if ($Matches[0] -notlike "*-*" -and $Matches[0] -notlike "*/*") { # Ignore if its already an IP range or cidr range
										$IPAddressInfo = $IPAddressInfo -replace "(?<start>(^|>)(External )?IP(<.*>)?:?(<.*>)? *)(?<ip>$IPRegex)(?<end> *?($|<))", ('${start}${ip}/' + $CidrRange + '${end}')
									}
								}
							}
						}
						$IPHTML = $IPAddressInfo -replace "Subnet ?(Mask)?(<.*>)?:?(<.*>)? *$IPRegex", '' -replace "DNS( IP)?(v4|v6)?( \d)?(<.*>)?:?(<.*>)? *$IPRegex", ''
					} else {
						$IPHTML = $IPAddressInfo
					}
		
					# Find all IP's in the html, parse ranges and masks if needed, then map them to their locations
					$Matches = [RegEx]::Matches($IPHTML, $IPRegex)

					if ($Matches -and $Matches.value) {
						$IPs = @($Matches.Value)
						foreach ($IP in $IPs) {
							$FoundIPs = @()
							if ($IP -like "*-*" -and $IP -like "*/*") {
								$IP = $IP -replace '(\/[1-3][0-9])$', ''
							}
							if ($IP -like "*-*") {
								$IPRange = $IP -split '-'
								$Octets = $IPRange[0] -split '\.'
								$RangeFrom = $Octets[3]
								$RangeTo = $IPRange[1]
								$AllEndingOctets = $RangeFrom..$RangeTo
		
								foreach ($EndingOctet in $AllEndingOctets) {
									$FoundIPs += "$($Octets[0]).$($Octets[1]).$($Octets[2]).$($EndingOctet)"
								}
							} elseif ($IP -like "*/*") {
								$IPRange = Get-Subnet $IP
								$FoundIPs += @($IPRange.IPAddress.IPAddressToString)
								$IPRange.HostAddresses | ForEach-Object {
									$FoundIPs += $_
								}
								$FoundIPs += $IPRange.BroadcastAddress.IPAddressToString
							} else {
								$FoundIPs += $IP
							}

							$IPs_Parsed += $FoundIPs
							
							foreach ($FoundIP in $FoundIPs) {
								if (!$IPs_To_WAN[$FoundIP]) {
									$IPs_To_WAN[$FoundIP] = @()
								}
								$IPs_To_WAN[$FoundIP] += $WAN.id
							}
						}
					}
				}

				$InternalIPs = @()
				$ValidLANs = @()
				if ($LocationLANs) {
					foreach ($LAN in $LocationLANs) {
						if (!$LANIPs[$LAN.id]) {
							$LANIPs[$LAN.id] = @()
							$Subnets = $LAN.attributes.traits.subnet
							$IPMatches = [RegEx]::Matches($Subnets, $IPRegex)

							if ($IPMatches -and $IPMatches.value) {
								$SubnetIPs = @($IPMatches.value)
								foreach ($SubnetIP in $SubnetIPs) {
									$IPSubnet = Get-Subnet $SubnetIP
									$ValidLANs += $LAN.id
									$IPSubnet.HostAddresses | ForEach-Object {
										$LANIPs[$LAN.id] += $_
										$InternalIPs += $_
									}
								}
							}
						} else {
							$InternalIPs += $LANIPs[$LAN.id]
						}
					}
				}
				$ValidLANs = $ValidLANs | Select-Object -Unique

				if (!$IPs_Parsed) {
					continue
				}
				
				$AutotaskLocation = $false
				if ($AutotaskConnected -and $Autotask_Locations) {
					$AutotaskLocation = $Autotask_Locations | Where-Object { $_.name -like $Location.attributes.name }
					if (!$AutotaskLocation) {
						$AutotaskLocation = $Autotask_Locations | Where-Object {
							$_.address1 -like $Location.attributes.'address-1' -and
							$_.address2 -like $Location.attributes.'address-2' -and
							$_.city -like $Location.attributes.city -and
							$_.postalCode -like $Location.attributes.'postal-code' -and
							$_.state -like $Location.attributes.'region-name' -and
							($_.phone -replace "[^0-9]") -like $Location.attributes.phone
						}
					}
				}

				$LocationIPs += [PSCustomObject]@{
					ExternalIPs = $IPs_Parsed | Sort-Object -Unique
					InternalIPs = $InternalIPs | Sort-Object -Unique
					ITGLocation = $Location.id
					AutotaskLocation = if ($AutotaskLocation) { $AutotaskLocation.id } else { $false }
					WANs = @($LocationWANs.id)
					LANs = @($ValidLANs)
					IPs_To_WAN = $($IPs_To_WAN)
				}
			}

			if ($LocationIPsLocation) {
				if (!(Test-Path -Path $LocationIPsLocation)) {
					New-Item -ItemType Directory -Force -Path $LocationIPsLocation | Out-Null
				}

				$LocationIPsPath = "$($LocationIPsLocation)\$($Company_Acronym)_location_ips.json"
				$LocationIPs | ConvertTo-Json | Out-File -FilePath $LocationIPsPath
			}

			# Prep overview lists
			$WANDevices = @{}
			$LANDevices = @{}
			foreach ($LocationInfo in $LocationIPs) {
				foreach ($WAN in $LocationInfo.WANs) {
					if (!$WANDevices[$WAN]) {
						$WANDevices[$WAN] = @()
					}
				}
				foreach ($LAN in $LocationInfo.LANs) {
					if (!$LANDevices[$LAN]) {
						$LANDevices[$LAN] = @()
					}
				}
			}

			# We have all the locations mapped to ip lists, lets go through the list of devices and determine each one's location
			if ($LocationIPs) {
				$i = 0
				$MatchedDeviceCount = ($MatchedDevices | Measure-Object).Count
				foreach ($MatchedDevice in $MatchedDevices) {
					$i++
					[int]$PercentComplete = ($i / $MatchedDeviceCount * 100)
					$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
					Write-Progress -Activity "Updating Device Locations" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "% (Checking: $Hostname)")

					if (!$MatchedDevice.itg_matches -and (!$MatchedDevice.autotask_matches -or !$MatchedDevice.rmm_matches)) {
						continue
					}

					$RMMDevice = @()
					$ExternalIP = $false
					foreach ($DeviceID in $MatchedDevice.rmm_matches) {
						$RMMDevice += $RMM_DevicesHash[$DeviceID]
					}
					if ($RMMDevice) {
						$ExternalIP = @($RMMDevice.extIpAddress)
						$InternalIP = @($RMMDevice.intIpAddress)
					} else {
						$AutotaskDevice = @()
						foreach ($DeviceID in $MatchedDevice.autotask_matches) {
							$AutotaskDevice += $Autotask_DevicesHash[$DeviceID]
						}
						if ($AutotaskDevice) {
							$ExternalIP = @($AutotaskDevice.rmmDeviceAuditExternalIPAddress)
							$InternalIP = @($AutotaskDevice.rmmDeviceAuditIPAddress)
						}
					}

					if (!$ExternalIP) {
						continue
					}

					$PossibleLocations = @()
					$ExternalIP | ForEach-Object {
						$IP = $_;
						$PossibleLocations += $LocationIPs | Where-Object { $_.ExternalIPs -contains $IP }
					}

					if (($PossibleLocations.ITGLocation | Select-Object -Unique | Measure-Object).Count -gt 1) {
						# if more than 1 possible location, try narrowing down by internal ip
						$PossibleLocations_IntFiltered = @()
						$InternalIP | ForEach-Object {
							$IP = $_;
							$PossibleLocations_IntFiltered += $PossibleLocations | Where-Object { $_.InternalIPs -contains $IP }
						}
						if (($PossibleLocations_IntFiltered | Measure-Object).Count -gt 0) {
							$PossibleLocations = $PossibleLocations_IntFiltered
						}
					}

					$PossibleLocations = $PossibleLocations | Where-Object { $_.AutotaskLocation -or $_.ITGLocation }

					if (!$PossibleLocations) {
						continue
					}
					
					# Populate WAN and LAN device lists for custom overviews
					foreach ($ITG_DeviceID in $MatchedDevice.itg_matches) {
						$ITGMatch = $ITG_DevicesHash[$ITG_DeviceID]

						# If currently set location is in $PossibleLocations, use existing location
						if ($ITGMatch.attributes.'location-id' -and $ITGMatch.attributes.'location-id' -in $PossibleLocations.ITGLocation) {
							$ExistingLocation = $PossibleLocations | Where-Object { $ITGMatch.'location-id' -in $_.Location } | Select-Object -First 1
							if ($ExistingLocation.WANs) {
								$DeviceWANs = @()
								foreach ($Device_ExtIP in $ExternalIP) {
									$DeviceWANs += $ExistingLocation.IPs_To_WAN[$Device_ExtIP]
								}
								$DeviceWANs = $DeviceWANs | Sort-Object -Unique
								foreach ($WAN_ID in $DeviceWANs) {
									$WANDevices[$WAN_ID] += $MatchedDevice.id
								}
							}
						} else {
							# Otherwise use the newly chosen location
							$NewLocation = $PossibleLocations | Select-Object -First 1
							if ($NewLocation.WANs) {
								$DeviceWANs = @()
								foreach ($Device_ExtIP in $ExternalIP) {
									$DeviceWANs += $NewLocation.IPs_To_WAN[$Device_ExtIP]
								}
								$DeviceWANs = $DeviceWANs | Sort-Object -Unique
								foreach ($WAN_ID in $DeviceWANs) {
									$WANDevices[$WAN_ID] += $MatchedDevice.id
								}
							}
						}

						# Populate LAN info if applicable
						if ($InternalIP -in $PossibleLocations.InternalIPs) {
							foreach ($LAN_ID in $PossibleLocations.LANs) {
								$AllowedIPs = $LANIPs[$LAN_ID]
								if ($InternalIP -in $AllowedIPs) {
									$LANDevices[$LAN_ID] += $MatchedDevice.id
									break
								}
							}
						}
					}

					# Update locations in Autotask
					if ($AutotaskConnected -and $Autotask_Locations -and $PossibleLocations.AutotaskLocation) {
						foreach ($Autotask_DeviceID in $MatchedDevice.autotask_matches) {
							$AutotaskMatch = $Autotask_DevicesHash[$Autotask_DeviceID]
							# If currently set location is in $PossibleLocations, dont update
							if ($AutotaskMatch.companyLocationID -in $PossibleLocations.AutotaskLocation) {
								continue
							}

							# Update location
							Write-Progress -Activity "Updating Device Locations" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "% (Updating in Autotask: $Hostname)")
							$NewLocation = $PossibleLocations | Select-Object -First 1 # if multiple, just use the first

							$ConfigurationUpdate = 
							[PSCustomObject]@{
								companyLocationID = $NewLocation.AutotaskLocation
							}

							Set-AutotaskAPIResource -Resource ConfigurationItems -ID $Autotask_DeviceID -body $ConfigurationUpdate | Out-Null
						}
					}

					# Update Locations in ITG
					if ($PossibleLocations.ITGLocation) {
						foreach ($ITG_DeviceID in $MatchedDevice.itg_matches) {
							$ITGMatch = $ITG_DevicesHash[$ITG_DeviceID]
							$DeviceLocationsUpdateRan = $true
							# If currently set location is in $PossibleLocations, dont update
							if ($ITGMatch.attributes.'location-id' -in $PossibleLocations.ITGLocation) {
								continue
							}
							# If the device is synced with autotask then we cannot update ITG directly, instead this will sync down from Autotask which we updated above
							if ($ITGMatch.attributes.'psa-integration' -and $ITGMatch.attributes.'psa-integration' -ne 'disabled') {
								continue
							}

							# Update location
							Write-Progress -Activity "Updating Device Locations" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "% (Updating in ITG: $Hostname)")
							$NewLocation = $PossibleLocations | Select-Object -First 1 # if multiple, just use the first

							$ConfigurationUpdate = @{
								'type' = 'configurations'
								'attributes' = @{
									'location-id' = $NewLocation.ITGLocation
								}
							}

							Set-ITGlueConfigurations -id $ITG_DeviceID -data $ConfigurationUpdate | Out-Null
						}
					}
				}
				Write-Progress -Activity "Updating Device Locations" -Status "Ready" -Completed
			}

			if ($WAN_LAN_HistoryLocation) {
				if (!(Test-Path -Path $WAN_LAN_HistoryLocation)) {
					New-Item -ItemType Directory -Force -Path $WAN_LAN_HistoryLocation | Out-Null
				}
			}

			# Create custom WAN & LAN overviews
			if ($WANDevices -and $UpdateOverview) {
				foreach ($WAN_ID in $WANDevices.keys) {
					$WAN = $WANs | Where-Object { $_.id -eq $WAN_ID }
					$Title = "WAN: Seen Devices - $($WAN.attributes.traits.label)"
					$ExistingOverview = $WANCustomOverviews | Where-Object { $_.attributes.traits.label -like $Title -or $_.attributes.traits.overview -like "*WAN ID: '$WAN_ID'*" } | Sort-Object -Property {$_.attributes.'updated-at'} -Descending | Select-Object -First 1

					$DeviceTable = @()
					$DeviceHistory = @()

					# Get previously seen devices first
					$HistoryPath = "$($WAN_LAN_HistoryLocation)\$($Company_Acronym)_wan_$($WAN_ID)_history.json"
					if (Test-Path $HistoryPath) {
						$PreviousWANHistory = $true
						$DevicePreviousHistory = Get-Content -Path $HistoryPath -Raw | ConvertFrom-Json
					}

					# get list of recently seen devices
					foreach ($MatchID in $WANDevices.Item($WAN_ID)) {
						$MatchedDevice = $MatchedDevicesHash[$MatchID]
						$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
						$ITG_DeviceID = $null
						if (($MatchedDevice.itg_matches | Measure-Object).Count -gt 0) {
							$ITG_Device = $ITG_DevicesHash[($MatchedDevice.itg_matches | Select-Object -First 1)]
							$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"
							$ITG_DeviceID = $ITG_Device.id
						}

						if ($PreviousWANHistory -and ($Hostname -in $DevicePreviousHistory -or ($ITG_DeviceID -and $ITG_DeviceID -in $DevicePreviousHistory.DeviceID))) {
							# Remove devices from previously seen if in current list
							$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceName -ne $Hostname }
							if ($ITG_DeviceID) {
								$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceID -ne $ITG_DeviceID }
							}
						}

						if ($Hostname -in $DeviceHistory.DeviceName) {
							continue
						}

						$ActivityComparison = $MatchedDevice.activity_comparison
						$Activity = $ActivityComparison.Values | Sort-Object last_active
						$LastSeen = ''
						$LastSeenUTC = $null
						if (($Activity | Measure-Object).count -gt 1) {
							$LastIndex = ($Activity | Measure-Object).count-1
							$LastSeen = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
							$LastSeenUTC = $LastSeen.ToUniversalTime()
						}

						$Row = [PSCustomObject]@{
							'DeviceName' = $Hostname
							'Device' = $HostnameAndURL
							'Last Seen' = $LastSeen
						}
						$DeviceTable += $Row

						$DeviceHistory += [PSCustomObject]@{
							'DeviceID' = $ITG_DeviceID
							'DeviceName' = $Hostname
							'LastSeen' = $LastSeenUTC
							'Type' = 'Current'
						}
					}

					if (!$DeviceTable) {
						$DeviceTable = "None seen yet."
						$Overview = "<p>None seen yet.</p>"
					} else {
						$DeviceTable = ($DeviceTable | Sort-Object -Property  @{expression = 'Last Seen'; descending = $true}, @{expression = 'DeviceName'; descending = $false})
						$Overview = $DeviceTable | Select-Object 'Device', 'Last Seen' | ConvertTo-Html -Fragment
					}

					# Create previous device history table
					if ($PreviousWANHistory -and ($DevicePreviousHistory | Measure-Object).Count -gt 0) {
						$DeviceHistoryTable = @()
						foreach ($Device in $DevicePreviousHistory) {
							if ($Device.DeviceID) {
								$MatchedDevice = $MatchedDevices | Where-Object { $_.itg_matches -contains $Device.DeviceID } | Select-Object -First 1
							} else {
								$MatchedDevice = $MatchedDevices | Where-Object { $_.itg_hostname -contains $Device.DeviceName } | Select-Object -First 1
							}

							if (!$MatchedDevice) {
								continue
							}

							$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
							$ITG_Device = $ITG_DevicesHash[($MatchedDevice.itg_matches | Select-Object -First 1)]
							$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"

							if ($Hostname -in $DeviceHistory.DeviceName -or ($Device.DeviceID -and $Device.DeviceID -in $DeviceHistory.DeviceID)) {
								continue
							}

							$Device.LastSeen = $Device.LastSeen -as [DateTime];

							$Row = [PSCustomObject]@{
								'DeviceName' = $Hostname
								'Device' = $HostnameAndURL
								'Last Seen' = if ($Device.LastSeen) { $Device.LastSeen.ToLocalTime() } else { $false }
							}
							$DeviceHistoryTable += $Row

							$DeviceHistory += [PSCustomObject]@{
								'DeviceID' = $ITG_Device.id
								'DeviceName' = $Hostname
								'LastSeen' = $Device.LastSeen
								'Type' = 'Previous'
							}
						}
						$DeviceHistoryTable = ($DeviceHistoryTable | Sort-Object -Property  @{expression = 'Last Seen'; descending = $true}, @{expression = 'DeviceName'; descending = $false})

						if ($DeviceHistoryTable) {
							$Overview += "`n`n<h3>Previously Seen Devices</h3> `n"
							$Overview += $DeviceHistoryTable | Select-Object 'Device', 'Last Seen' | ConvertTo-Html -Fragment
						}
					}

					$Overview += "`n<p>WAN ID: '$WAN_ID'</p>"

					# Export devices to json file so we can track devices seen previously in this WAN
					if ($WAN_LAN_HistoryLocation) {
						$HistoryPath = "$($WAN_LAN_HistoryLocation)\$($Company_Acronym)_wan_$($WAN_ID)_history.json"
						$DeviceHistory | ConvertTo-Json | Out-File -FilePath $HistoryPath
						Write-Host "Exported the wan device history: $($WAN.attributes.traits.label)."
					}

					if ($ExistingOverview) {
						# Update existing in ITG
						$FlexAssetBody = 
						@{
							type = 'flexible-assets'
							attributes = @{
								traits = @{
									"name" = $Title
									"overview" = [System.Web.HttpUtility]::HtmlDecode($Overview)
								}
							}
						}
						Set-ITGlueFlexibleAssets -id $ExistingOverview.id -data $FlexAssetBody
						$OverviewID = $ExistingOverview.id
						$ExistingOverview = Get-ITGlueFlexibleAssets -id $OverviewID -include related_items
					} else {
						# Upload new to ITG
						$FlexAssetBody = 
						@{
							type = 'flexible-assets'
							attributes = @{
								'organization-id' = $ITG_ID
								'flexible-asset-type-id' = $OverviewFilterID.id
								traits = @{
									"name" = $Title
									"overview" = [System.Web.HttpUtility]::HtmlDecode($Overview)
								}
							}
						}
						$New_WANOverview = New-ITGlueFlexibleAssets -data $FlexAssetBody
						$OverviewID = $New_WANOverview.data.id
					}

					# Add related items
					$RelatedItemsBody = @()
					$RelatedItemsBody +=
					@{
						type = 'related_items'
						attributes = @{
							'destination_id' = $WAN_ID
							'destination_type' = "Flexible Asset"
						}
					}
					foreach ($Device in $DeviceHistory) {
						$ITG_Device = $ITG_DevicesHash[$Device.DeviceID]

						$Note = ''
						if ($Device.Type -eq 'Current') {
							$Note = 'Current WAN'
						} else {
							$Note = 'Previously seen on WAN'
						}

						if ($ExistingOverview.included -and $ITG_Device.id -in $ExistingOverview.included.attributes.'resource-id') {
							$RelatedItem = $ExistingOverview.included | Where-Object { $_.attributes.'resource-id' -contains $ITG_Device.id }
							if ($RelatedItem.attributes.notes -like $Note) {
								continue
							} else {
								# Update related item
								$Delete_RelatedItemsBody = @()
								$RelatedItem.id | ForEach-Object {
									$Delete_RelatedItemsBody +=
									@{
										type = 'related_items'
										attributes = @{
											'id' = $_
										}
									}
								}
								Remove-ITGlueRelatedItems -resource_type 'flexible_assets' -resource_id $OverviewID -data $Delete_RelatedItemsBody
							}
						}

						if ($ITG_Device -and $ITG_Device.id) {
							$RelatedItemsBody +=
							@{
								type = 'related_items'
								attributes = @{
									'destination_id' = $ITG_Device.id
									'destination_type' = "Configuration"
									'notes' = $Note
								}
							}
						}
					}

					New-ITGlueRelatedItems -resource_type 'flexible_assets' -resource_id $OverviewID -data $RelatedItemsBody
				}
			}

			if ($LANDevices -and $UpdateOverview) {
				foreach ($LAN_ID in $LANDevices.keys) {
					$LAN = $LANs | Where-Object { $_.id -eq $LAN_ID }
					$Title = "LAN: Seen Devices - $($LAN.attributes.traits.name)"
					$ExistingOverview = $LANCustomOverviews | Where-Object { $_.attributes.traits.name -like $Title -or $_.attributes.traits.overview -like "*LAN ID: '$LAN_ID'*" } | Sort-Object -Property {$_.attributes.'updated-at'} -Descending | Select-Object -First 1
					$DeviceTable = @()
					$DeviceHistory = @()

					# Get previously seen devices first
					$HistoryPath = "$($WAN_LAN_HistoryLocation)\$($Company_Acronym)_lan_$($LAN_ID)_history.json"
					if (Test-Path $HistoryPath) {
						$PreviousLANHistory = $true
						$DevicePreviousHistory = Get-Content -Path $HistoryPath -Raw | ConvertFrom-Json
					}

					# get list of recently seen devices
					foreach ($MatchID in $LANDevices.Item($LAN_ID)) {
						$MatchedDevice = $MatchedDevicesHash[$MatchID]
						$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
						$ITG_DeviceID = $null
						if (($MatchedDevice.itg_matches | Measure-Object).Count -gt 0) {
							$ITG_Device = $ITG_DevicesHash[($MatchedDevice.itg_matches | Select-Object -First 1)]
							$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"
							$ITG_DeviceID = $ITG_Device.id
						}

						if ($PreviousLANHistory -and ($Hostname -in $DevicePreviousHistory -or ($ITG_DeviceID -and $ITG_DeviceID -in $DevicePreviousHistory.DeviceID))) {
							# Remove devices from previously seen if in current list
							$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceName -ne $Hostname }
							if ($ITG_DeviceID) {
								$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceID -ne $ITG_DeviceID }
							}
						}

						if ($Hostname -in $DeviceHistory.DeviceName) {
							continue
						}

						$ActivityComparison = $MatchedDevice.activity_comparison
						$Activity = $ActivityComparison.Values | Sort-Object last_active
						$LastSeen = ''
						if (($Activity | Measure-Object).count -gt 1) {
							$LastIndex = ($Activity | Measure-Object).count-1
							$LastSeen = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
						}

						$Row = [PSCustomObject]@{
							'DeviceName' = $Hostname
							'Device' = $HostnameAndURL
							'Last Seen' = $LastSeen
						}
						$DeviceTable += $Row

						$DeviceHistory += [PSCustomObject]@{
							'DeviceID' = $ITG_DeviceID
							'DeviceName' = $Hostname
							'LastSeen' = $LastSeen.ToUniversalTime()
							'Type' = 'Current'
						}
					}

					if (!$DeviceTable) {
						$DeviceTable = "None seen yet."
						$Overview = "<p>None seen yet.</p>"
					} else {
						$DeviceTable = ($DeviceTable | Sort-Object -Property  @{expression = 'Last Seen'; descending = $true}, @{expression = 'DeviceName'; descending = $false})
						$Overview = $DeviceTable | Select-Object 'Device', 'Last Seen' | ConvertTo-Html -Fragment
					}

					# Create previous device history table
					if ($PreviousLANHistory -and ($DevicePreviousHistory | Measure-Object).Count -gt 0) {
						$DeviceHistoryTable = @()
						foreach ($Device in $DevicePreviousHistory) {
							if ($Device.DeviceID) {
								$MatchedDevice = $MatchedDevices | Where-Object { $_.itg_matches -contains $Device.DeviceID } | Select-Object -First 1
							} else {
								$MatchedDevice = $MatchedDevices | Where-Object { $_.itg_hostname -contains $Device.DeviceName } | Select-Object -First 1
							}

							if (!$MatchedDevice) {
								continue
							}

							$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
							$ITG_Device = $ITG_DevicesHash[($MatchedDevice.itg_matches | Select-Object -First 1)]
							$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"

							if ($Hostname -in $DeviceHistory.DeviceName -or ($Device.DeviceID -and $Device.DeviceID -in $DeviceHistory.DeviceID)) {
								continue
							}

							$Device.LastSeen = $Device.LastSeen -as [DateTime];

							$Row = [PSCustomObject]@{
								'DeviceName' = $Hostname
								'Device' = $HostnameAndURL
								'Last Seen' = if ($Device.LastSeen) { $Device.LastSeen.ToLocalTime() } else { $false }
							}
							$DeviceHistoryTable += $Row

							$DeviceHistory += [PSCustomObject]@{
								'DeviceID' = $ITG_Device.id
								'DeviceName' = $Hostname
								'LastSeen' = $Device.LastSeen
								'Type' = 'Previous'
							}
						}
						$DeviceHistoryTable = ($DeviceHistoryTable | Sort-Object -Property  @{expression = 'Last Seen'; descending = $true}, @{expression = 'DeviceName'; descending = $false})

						if ($DeviceHistoryTable) {
							$Overview += "`n`n<h3>Previously Seen Devices</h3> `n"
							$Overview += $DeviceHistoryTable | Select-Object 'Device', 'Last Seen' | ConvertTo-Html -Fragment
						}
					}

					$Overview += "`n<p>LAN ID: '$LAN_ID'</p>"

					# Export devices to json file so we can track devices seen previously in this WAN
					if ($WAN_LAN_HistoryLocation) {
						$HistoryPath = "$($WAN_LAN_HistoryLocation)\$($Company_Acronym)_lan_$($LAN_ID)_history.json"
						$DeviceHistory | ConvertTo-Json | Out-File -FilePath $HistoryPath
						Write-Host "Exported the lan device history: $($LAN.attributes.traits.name)."
					}

					if ($ExistingOverview) {
						# Update existing in ITG
						$FlexAssetBody = 
						@{
							type = 'flexible-assets'
							attributes = @{
								traits = @{
									"name" = $Title
									"overview" = [System.Web.HttpUtility]::HtmlDecode($Overview)
								}
							}
						}
						Set-ITGlueFlexibleAssets -id $ExistingOverview.id -data $FlexAssetBody
						$OverviewID = $ExistingOverview.id
						$ExistingOverview = Get-ITGlueFlexibleAssets -id $OverviewID -include related_items
					} else {
						# Upload new to ITG
						$FlexAssetBody = 
						@{
							type = 'flexible-assets'
							attributes = @{
								'organization-id' = $ITG_ID
								'flexible-asset-type-id' = $OverviewFilterID.id
								traits = @{
									"name" = $Title
									"overview" = [System.Web.HttpUtility]::HtmlDecode($Overview)
								}
							}
						}
						$New_LANOverview = New-ITGlueFlexibleAssets -data $FlexAssetBody
						$OverviewID = $New_LANOverview.data.id
					}
			
					# Add related items
					$RelatedItemsBody = @()
					$RelatedItemsBody +=
					@{
						type = 'related_items'
						attributes = @{
							'destination_id' = $LAN_ID
							'destination_type' = "Flexible Asset"
						}
					}
					foreach ($Device in $DeviceHistory) {
						$ITG_Device = $ITG_DevicesHash[$Device.DeviceID]

						$Note = ''
						if ($Device.Type -eq 'Current') {
							$Note = 'Current LAN'
						} else {
							$Note = 'Previously seen on LAN'
						}

						if ($ExistingOverview.included -and $ITG_Device.id -in $ExistingOverview.included.attributes.'resource-id') {
							$RelatedItem = $ExistingOverview.included | Where-Object { $_.attributes.'resource-id' -contains $ITG_Device.id }
							if ($RelatedItem.attributes.notes -like $Note) {
								continue
							}
						}

						if ($ITG_Device -and $ITG_Device.id) {
							$RelatedItemsBody +=
							@{
								type = 'related_items'
								attributes = @{
									'destination_id' = $ITG_Device.id
									'destination_type' = "Configuration"
									'notes' = $Note
								}
							}
						}
					}

					New-ITGlueRelatedItems -resource_type 'flexible_assets' -resource_id $OverviewID -data $RelatedItemsBody
				}
			}
		}

		Write-Host "Device locations updated."
		Write-Host "======================"
	}

	# Update / Create the "Scripts - Last Run" ITG page which shows when the device audit (and other scripts) last ran
	if ($LastUpdatedUpdater_APIURL -and $ITG_ID) {
		if ($ScriptsLastRunFilterID) {
			$LastUpdatedPage = Get-ITGlueFlexibleAssets -filter_flexible_asset_type_id $ScriptsLastRunFilterID.id -filter_organization_id $ITG_ID
		
			if (!$LastUpdatedPage -or !$LastUpdatedPage.data) {
				# Upload new to ITG
				$FlexAssetBody = 
				@{
					type = 'flexible-assets'
					attributes = @{
						'organization-id' = $ITG_ID
						'flexible-asset-type-id' = $ScriptsLastRunFilterID.id
						traits = @{
							"name" = "Scripts - Last Run"
							"current-version" = "N/A"
						}
					}
				}
				$LastUpdatedPage = New-ITGlueFlexibleAssets -data $FlexAssetBody
				Write-Host "Created a new 'Scripts - Last Run' page."
			}
		}

		$Headers = @{
			"x-api-key" = $ITGAPIKey.Key
		}
		$Body = @{
			"apiurl" = $ITGAPIKey.Url
			"itgOrgID" = $ITG_ID
			"HostDevice" = $env:computername
		}

		# Update asset with last run times for the device audit
		if ($DeviceIssueCheckRan) {
			$Body.Add("device-cleanup", (Get-Date).ToString("yyyy-MM-dd"))
		}
		if ($DeviceUsageUpdateRan) {
			$Body.Add("device-usage", (Get-Date).ToString("yyyy-MM-dd"))
		}
		if ($DeviceLocationsUpdateRan) {
			$Body.Add("device-locations", (Get-Date).ToString("yyyy-MM-dd"))
		}
		if ($MonthlyStatsUpdated) {
			$Body.Add("monthly-stats-rollup", (Get-Date).ToString("yyyy-MM-dd"))
		}
		if ($DeviceUsersUpdateRan) {
			$Body.Add("device-users", (Get-Date).ToString("yyyy-MM-dd"))
		}

		$Params = @{
			Method = "Post"
			Uri = $LastUpdatedUpdater_APIURL
			Headers = $Headers
			Body = ($Body | ConvertTo-Json)
			ContentType = "application/json"
		}			
		Invoke-RestMethod @Params
	}
}

# Cleanup
Disconnect-MgGraph