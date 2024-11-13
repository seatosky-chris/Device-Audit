param(
	$companies = @()
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
If (Get-Module -ListAvailable -Name "DattoRMM") {Import-module DattoRMM -Force} Else { install-module DattoRMM -Force; import-module DattoRMM -Force}

# Connect to IT Glue
$ITGConnected = $false
if ($ITGAPIKey.Key) {
	Add-ITGlueBaseURI -base_uri $ITGAPIKey.Url
	Add-ITGlueAPIKey $ITGAPIKey.Key
	$ScriptsLastRunFilterID = (Get-ITGlueFlexibleAssetTypes -filter_name $ScriptsLastRunFlexAssetName).data
	$ITGConnected = $true
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
	# $ServiceTarget is 'rmm', 'sc', 'sophos', 'jc', 'ninite', 'itg', or 'autotask'
	function log_change($Company_Acronym, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $JC_Device_ID = $false, $Ninite_Device_ID = $false, $ChangeType, $Hostname = "", $Reason = "") {
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
			ninite_id = $Ninite_Device_ID
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
	function log_query($LogHistory, $StartTime, $EndTime, $ServiceTarget = "", $RMM_Device_ID = "", $SC_Device_ID = "", $Sophos_Device_ID = "", $Ninite_Device_ID = "", $ChangeType = "", $Hostname = "", $Reason = "") {
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
		if ($Ninite_Device_ID) {
			$History_Filtered = $History_Filtered | Where-Object { $Ninite_Device_ID -in $_.ninite_id }
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
	function log_attempt_count($LogHistory, $ServiceTarget = "", $RMM_Device_ID = "", $SC_Device_ID = "", $Sophos_Device_ID = "", $Ninite_Device_ID = "", $ChangeType = "", $Hostname = "", $Reason = "") {
		$History = log_query -LogHistory $LogHistory -StartTime 0 -EndTime 'now' -ServiceTarget $ServiceTarget -RMM_Device_ID $RMM_Device_ID -SC_Device_ID $SC_Device_ID -Sophos_Device_ID $Sophos_Device_ID -Ninite_Device_ID $Ninite_Device_ID -ChangeType $ChangeType -Hostname $Hostname -Reason $Reason
		return ($History | Measure-Object).Count
	}

	# This function finds the difference in seconds between the oldest and newest unixtimestamp in a set of log history
	function log_time_diff($LogHistory) {
		$Newest = $LogHistory | Sort-Object -Property datetime -Descending | Select-Object -First 1
		$Oldest = $LogHistory | Sort-Object -Property datetime | Select-Object -First 1
		return $Newest.datetime - $Oldest.datetime
	}

	# Function for querying repair tickets based on the possible filters
	# $ServiceTarget is 'rmm', 'sc', 'sophos', or 'ninite'
	# $Hostname can be a single hostname or an array of hostnames to check for
	function repair_tickets($ServiceTarget = "", $Hostname = "") {
		if ($ServiceTarget -eq 'rmm') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "RMM *" -or $_.title -like "* RMM" -or $_.title -like "* RMM *" }
		} elseif ($ServiceTarget -eq 'sc') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "SC *" -or $_.title -like "* SC" -or $_.title -like "* SC *" -or $_.title -like "*ScreenConnect*" }
		} elseif ($ServiceTarget -eq 'sophos') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "Sophos *" -or $_.title -like "* Sophos" -or $_.title -like "* Sophos *" }
		} elseif ($ServiceTarget -eq 'ninite') {
			$RepairTickets_ByService = $RepairTickets | Where-Object { $_.title -like "Ninite *" -or $_.title -like "* Ninite" -or $_.title -like "* Ninite *" }
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
	function check_failed_attempts($LogHistory, $Company_Acronym, $ErrorMessage, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $Ninite_Device_ID, $ChangeType, $Hostname = "", $Reason = "") {
		$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
		$TenDays = [int](New-TimeSpan -Start (Get-Date).AddDays(-10).ToUniversalTime() -End (Get-Date).ToUniversalTime()).TotalSeconds

		# first check if a repair ticket already exists for this device/service
		if ($ServiceTarget -in ("rmm", "sc", "sophos", "ninite")) {
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
		} elseif ($ServiceTarget -eq 'ninite') {
			$ID_Params."Ninite_Device_ID" = $Ninite_Device_ID
			$EmailLink = ""
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
	
	function compare_activity_ninite($DeviceIDs) {
		if (!$Ninite_DevicesHash) {
			return @()
		}

		$NiniteDevices = @()
		foreach ($DeviceID in $DeviceIDs) {
			$NiniteDevices += $Ninite_DevicesHash[$DeviceID]
		}
		$DevicesOutput = @()

		foreach ($Device in $NiniteDevices) {
			$DeviceOutputObj = [PsCustomObject]@{
				type = "ninite"
				id = $Device.id
				last_active = $null
			}

			if ($Device.last_seen -and [string]$Device.last_seen -as [DateTime]) {
				$DeviceOutputObj.last_active = [DateTime]$Device.last_seen
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

	# Helper function that takes a $MatchedDevices object and returns the activity comparison for SC, RMM, Sophos, and (if applicable) Azure, Intune, JC &/or Ninite
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

		if ($NiniteAuthResponse -and $MatchedDevice.ninite_matches -and $MatchedDevice.ninite_matches.count -gt 0) {
			$NiniteActivity = compare_activity_ninite $MatchedDevice.ninite_matches
			$Activity.ninite = $NiniteActivity | Sort-Object last_active -Descending | Select-Object -First 1
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
	# Set $ToInstall to what needs to be installed ("sc", "rmm", or "ninite")
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
	# Set $ToInstall to what needed to be installed ("sc", "rmm", or "ninite"), if left $false will remove for all $ToInstall types
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
	# $System should be 'sc', 'rmm', 'sophos', or 'ninite'
	function ignore_install($Device, $System) {
		if ($System -eq 'sc' -and $Ignore_Installs.SC -eq $true) {
			return $true
		} elseif ($System -eq 'rmm' -and $Ignore_Installs.RMM -eq $true) {
			return $true
		} elseif ($System -eq 'sophos' -and $Ignore_Installs.Sophos -eq $true) {
			return $true
		} elseif ($System -eq 'ninite' -and $Ignore_Installs.Ninite -eq $true) {
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
		} elseif ($System -eq 'ninite') {
			$IgnoredDevices = $Ignore_Installs.Ninite
		}

		if ($IgnoredDevices) {
			if ($System -eq 'sc' -and ($IgnoredDevices | Where-Object { $_ -in $Device.rmm_hostname -or $_ -in $Device.rmm_matches -or $_ -in $Device.sophos_hostname -or $_ -in $Device.sophos_matches } | Measure-Object).Count -gt 0) {
				return $true
			} elseif ($System -eq 'rmm' -and ($IgnoredDevices | Where-Object { $_ -in $Device.sc_hostname -or $_ -in $Device.sc_matches -or $_ -in $Device.sophos_hostname -or $_ -in $Device.sophos_matches } | Measure-Object).Count -gt 0) {
				return $true
			} elseif ($System -eq 'sophos' -and ($IgnoredDevices | Where-Object { $_ -in $Device.sc_hostname -or $_ -in $Device.sc_matches -or $_ -in $Device.rmm_hostname -or $_ -in $Device.rmm_matches  } | Measure-Object).Count -gt 0) {
				return $true
			} elseif ($System -eq 'ninite' -and ($IgnoredDevices | Where-Object { $_ -in $Device.sc_hostname -or $_ -in $Device.sc_matches -or $_ -in $Device.rmm_hostname -or $_ -in $Device.rmm_matches -or $_ -in $Device.sophos_hostname -or $_ -in $Device.sophos_matches } | Measure-Object).Count -gt 0) {
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

	# Deletes a device from Ninite
	function delete_from_ninite($Ninite_ID, $NiniteWebSession, $NiniteHeader) {
		$FormBody = @{
			id = 1
			jsonrpc = "2.0"
			method = "delete_machine"
			params = @{
				machine_id = $Ninite_ID
			}
		} | ConvertTo-Json
	
		try {
			$NiniteResponse = Invoke-WebRequest "$($Ninite_Login.BaseURI)remote/rpc_web" -WebSession $NiniteWebSession -Headers $NiniteHeader -Body $FormBody -Method 'POST' -ContentType 'application/json; charset=utf-8'
		} catch {
			Write-Warning "Could not delete device '$($Ninite_ID)' from Ninite."
			return $false
		}
		$DeleteResponse = $NiniteResponse.Content | ConvertFrom-Json
		if ($DeleteResponse -and $null -eq $DeleteResponse.result) {
			return $true
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

	function install_ninite_using_rmm($RMM_Device) {
		if (is_existing_rmm_job_active -RMM_Device $RMM_Device -JobType "install_ninite") {
			return $false
		}

		if ($RMM_Device."Operating System" -like "*Windows*" -and $RMM_Device."Device Type" -notlike "*Server*") {
			$Job = Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Install Ninite on $($RMM_Device."Device Hostname")" -ComponentName "Ninite Agent Installer [WIN]"
			if ($Job -and $Job.job -and $Job.job.uid) {
				$Removed = remove_device_from_install_queue -RMM_ID $RMM_Device."Device UID" -ToInstall "ninite"
				log_recent_rmm_job -RMM_Device $RMM_Device -JobType "install_ninite" -JobID $Job.job.uid
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

	# Get install queue
	if (!(Test-Path -Path $InstallQueueLocation)) {
		New-Item -ItemType Directory -Force -Path $InstallQueueLocation | Out-Null
	}
	$InstallQueuePath = "$($InstallQueueLocation)\$($Company_Acronym)_install_queue.json"
	$InstallQueue = $false
	if (Test-Path -Path $InstallQueuePath) {
		$InstallQueue = Get-Content -Path $InstallQueuePath -Raw | ConvertFrom-Json
	}

	# Check to make sure there are installs to action
	$PossibleTypes = @("sc", "rmm", "ninite")
	$HasInstalls = $false
	if ($InstallQueue.PSObject.Properties) {
		foreach ($Type in $PossibleTypes) {
			if ($HasInstalls) { break }
			foreach ($Type2 in $PossibleTypes) {
				if ($InstallQueue.PSObject.Properties.Name -contains $Type -and $InstallQueue.($Type).PSObject.Properties -and $InstallQueue.($Type).PSObject.Properties.Name -contains $Type2 -and $InstallQueue.($Type).($Type2).Count -gt 0) {
					$HasInstalls = $true
					break
				}
			}
		}
	}

	if (!$HasInstalls) {
		continue
	}

	# Get recent RMM jobs log
	if (!(Test-Path -Path $RecentRMMJobsLocation)) {
		New-Item -ItemType Directory -Force -Path $RecentRMMJobsLocation | Out-Null
	}
	$RecentRMMJobsPath = "$($RecentRMMJobsLocation)\$($Company_Acronym)_recent_rmm_jobs.json"
	

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

	# Get the existing log
	$LogFilePath = "$($LogLocation)\$($Company_Acronym)_log.json"
	if ($LogLocation -and (Test-Path -Path $LogFilePath)) {
		$LogHistory = Get-Content -Path $LogFilePath -Raw | ConvertFrom-Json
	} else {
		$LogHistory = @{}
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

	# Attempt installs for devices in $InstallQueue
	if ($InstallQueue.PSObject.Properties) {
		# Install RMM by SC
		if ($InstallQueue.PSObject.Properties.Name -contains 'rmm' -and $InstallQueue.'rmm'.PSObject.Properties -and $InstallQueue.'rmm'.PSObject.Properties.Name -contains 'sc' -and $InstallQueue.'rmm'.'sc'.Count -gt 0) {
			foreach ($DeviceID in ($InstallQueue.'rmm'.'sc' | Sort-Object -Unique)) {
				$SCDevice = $SC_DevicesHash[$DeviceID]

				$4HoursAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddHours(-4).ToUniversalTime()).TotalSeconds
				$LogQuery_Params = @{
					LogHistory = $LogHistory
					StartTime = $4HoursAgo
					EndTime = 'now'
					ServiceTarget = 'sc'
					ChangeType = "install_rmm"
					SC_Device_ID = $SCDevice.SessionID
					Hostname = $SCDevice.Name
				}
				$Install_Logs = log_query @LogQuery_Params
				$LogCount = ($Install_Logs | Measure-Object).Count

				if ($LogCount -eq 0 -and $SCDevice -and $SCDevice.GuestLastSeen -gt (Get-Date).AddMinutes(-30)) {
					$LogParams = @{
						ServiceTarget = "sc"
						SC_Device_ID = $SCDevice.SessionID
						ChangeType = "install_rmm"
						Hostname = $SCDevice.Name
					}
					$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
					$EmailError = "Attempted reinstall of RMM on $($LogParams.Hostname). The Device Audit script has tried to reinstall RMM via SC $AttemptCount times now but it has not succeeded."
					$LogParams.Reason = "RMM reinstall attempted."

					if ($SCDevice.GuestOperatingSystemName -like "*Windows*" -and $SCDevice.GuestOperatingSystemName -notlike "*Windows Embedded*") {
						if (install_rmm_using_sc -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
							$AutoFix = $true
							check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
							log_change @LogParams -Company_Acronym $Company_Acronym
						}
					} elseif ($SCDevice.GuestOperatingSystemName -like "*Mac OS*") {
						if (install_rmm_using_sc_mac -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
							$AutoFix = $true
							check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
							log_change @LogParams -Company_Acronym $Company_Acronym
						}
					}
				}
			}
		}

		# Install SC by RMM
		if ($InstallQueue.PSObject.Properties.Name -contains 'sc' -and $InstallQueue.'sc'.PSObject.Properties -and $InstallQueue.'sc'.PSObject.Properties.Name -contains 'rmm' -and $InstallQueue.'sc'.'rmm'.Count -gt 0) {
			foreach ($DeviceID in ($InstallQueue.'sc'.'rmm' | Sort-Object -Unique)) {
				$RMMDevice = $RMM_DevicesHash[$DeviceID]

				if ($RMMDevice.suspended -ne "True") {
					$8HoursAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddHours(-8).ToUniversalTime()).TotalSeconds
					$LogQuery_Params = @{
						LogHistory = $LogHistory
						StartTime = $8HoursAgo
						EndTime = 'now'
						ServiceTarget = 'rmm'
						ChangeType = "install_sc"
						RMM_Device_ID = $RMMDevice."Device UID"
						Hostname = $RMMDevice."Device Hostname"
					}
					$Install_Logs = log_query @LogQuery_Params
					$LogCount = ($Install_Logs | Measure-Object).Count
					
					if ($LogCount -eq 0 -and ($RMMDevice.Status -eq "Online" -or $RMMDevice."Last Seen" -eq "Currently Online" -or ($RMMDevice."Last Seen" -as [DateTime]) -gt (Get-Date).AddMinutes(-30))) {
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
							$EmailError = "Attempted reinstall of SC on $($LogParams.Hostname). The Device Audit script has tried to reinstall SC via RMM $AttemptCount times now but it has not succeeded."
							$LogParams.Reason = "SC reinstall attempted."

							$AutoFix = $true
							check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
							log_change @LogParams -Company_Acronym $Company_Acronym
						}
					}
				}
			}
		}

		# Install Ninite by RMM
		if ($InstallQueue.PSObject.Properties.Name -contains 'ninite' -and $InstallQueue.'ninite'.PSObject.Properties -and $InstallQueue.'ninite'.PSObject.Properties.Name -contains 'rmm' -and $InstallQueue.'ninite'.'rmm'.Count -gt 0) {
			foreach ($DeviceID in ($InstallQueue.'ninite'.'rmm' | Sort-Object -Unique)) {
				$RMMDevice = $RMM_DevicesHash[$DeviceID]

				if ($RMMDevice.suspended -ne "True") {
					$8HoursAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddHours(-8).ToUniversalTime()).TotalSeconds
					$LogQuery_Params = @{
						LogHistory = $LogHistory
						StartTime = $8HoursAgo
						EndTime = 'now'
						ServiceTarget = 'rmm'
						ChangeType = "install_ninite"
						RMM_Device_ID = $RMMDevice."Device UID"
						Hostname = $RMMDevice."Device Hostname"
					}
					$Install_Logs = log_query @LogQuery_Params
					$LogCount = ($Install_Logs | Measure-Object).Count
					
					if ($LogCount -eq 0 -and ($RMMDevice.Status -eq "Online" -or $RMMDevice."Last Seen" -eq "Currently Online" -or ($RMMDevice."Last Seen" -as [DateTime]) -gt (Get-Date).AddMinutes(-30))) {
						$LogParams = @{
							ServiceTarget = "rmm"
							RMM_Device_ID = $RMMDevice."Device UID"
							ChangeType = "install_ninite"
							Hostname = $RMMDevice."Device Hostname"
						}

						if (install_ninite_using_rmm -RMM_Device $RMMDevice) {
							$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
							$EmailError = "Attempted reinstall of Ninite on $($LogParams.Hostname). The Device Audit script has tried to reinstall Ninite via RMM $AttemptCount times now but it has not succeeded."
							$LogParams.Reason = "Ninite reinstall attempted."

							$AutoFix = $true
							check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
							log_change @LogParams -Company_Acronym $Company_Acronym
						}
					}
				}
			}
		}
	}

	##############
	# Usage Updates
	##############

	# Save each user and the computer(s) they are using into the Usage database (for user audits and documenting who uses each computer)
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
				$User = $ExistingUsers | Where-Object { $_.Username -like $Username } | Sort-Object LastUpdated | Select-Object -First 1
	
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
	
		Write-Host "Usage Stats Saved!"
		Write-Host "===================="
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
		if ($DeviceUsageUpdateRan) {
			$Body.Add("device-usage", (Get-Date).ToString("yyyy-MM-dd"))
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