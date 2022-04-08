#####################################################################
### Load Variables from external file
### Make sure you setup your variables in the "Config Files/" folder
### Copy the template.ps1 file or another companies file and create a new one for the new company
### Set the companies config file to use below
. "$PSScriptRoot\Config Files\Global-Config.ps1" # Global Config
. "$PSScriptRoot\Config Files\APIKeys.ps1" # API Keys

. "$PSScriptRoot\Config Files\Config-NL.ps1" # Company config (CHANGE THIS)
#####################################################################

# This line allows popup boxes to work
Add-Type -AssemblyName PresentationFramework

Write-Host "Computer audit starting..."

$CurrentTLS = [System.Net.ServicePointManager]::SecurityProtocol
if ($CurrentTLS -notlike "*Tls12" -and $CurrentTLS -notlike "*Tls13") {
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
	Write-Host "This device is using an old version of TLS. Temporarily changed to use TLS v1.2."
}

# Import/Install any required modules
If (Get-Module -ListAvailable -Name "ImportExcel") {Import-module ImportExcel} Else { install-module ImportExcel -Force; import-module ImportExcel}
If (Get-Module -ListAvailable -Name "Az.Accounts") {Import-module Az.Accounts } Else { install-module Az.Accounts  -Force; import-module Az.Accounts }
If (Get-Module -ListAvailable -Name "Az.Resources") {Import-module Az.Resources } Else { install-module Az.Resources  -Force; import-module Az.Resources }
If (Get-Module -ListAvailable -Name "CosmosDB") {Import-module CosmosDB } Else { install-module CosmosDB  -Force; import-module CosmosDB }
If (Get-Module -ListAvailable -Name "ITGlueAPI") {Import-module ITGlueAPI -Force} Else { install-module ITGlueAPI -Force; import-module ITGlueAPI -Force}
If (Get-Module -ListAvailable -Name "AutotaskAPI") {Import-module AutotaskAPI -Force} Else { install-module AutotaskAPI -Force; import-module AutotaskAPI -Force}
If (Get-Module -ListAvailable -Name "Subnet") {Import-module Subnet -Force} Else { install-module Subnet -Force; import-module Subnet -Force}

# Connect to Azure
if (Test-Path "$PSScriptRoot\Config Files\AzureServiceAccount.json") {
	Import-AzContext -Path "$PSScriptRoot\Config Files\AzureServiceAccount.json"
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
	$ITGConnected = $true
}

# Connect to Autotask
$AutotaskConnected = $false
if ($AutotaskAPIKey.Key) {
	$Secret = ConvertTo-SecureString $AutotaskAPIKey.Key -AsPlainText -Force
	$Creds = New-Object System.Management.Automation.PSCredential($AutotaskAPIKey.Username, $Secret)
	Add-AutotaskAPIAuth -ApiIntegrationcode $AutotaskAPIKey.IntegrationCode -credentials $Creds
	Add-AutotaskBaseURI -BaseURI $AutotaskAPIKey.Url
	$AutotaskConnected = $true
}

if ($RMM_ID) {
	If (Get-Module -ListAvailable -Name "DattoRMM") {Import-module DattoRMM -Force} Else { install-module DattoRMM -Force; import-module DattoRMM -Force}
	Set-DrmmApiParameters -Url $DattoAPIKey.URL -Key $DattoAPIKey.Key -SecretKey $DattoAPIKey.SecretKey

	if ($RMM_ID -match "^\d+$") {
		$CompanyInfo = Get-DrmmAccountSites | Where-Object { $_.id -eq $RMM_ID }
		$RMM_ID = $CompanyInfo.uid
	}
}

if ($RMM_ID) {
	$RMM_Devices = Get-DrmmSiteDevices $RMM_ID | Where-Object { $_.deviceClass -eq 'device' -and $_.deviceType.category -in @("Laptop", "Desktop", "Server") }
} else {
	$RMM_Devices = Import-Csv $RMM_CSV
}

# Get all devices from ITG
$ITG_Devices = @()
if ($ITGConnected -and $ITG_ID) {
	$ITG_Devices = Get-ITGlueConfigurations -page_size 10000 -organization_id $ITG_ID
	if ($ITG_Devices -and $ITG_Devices.data) {
		$ITG_Devices = $ITG_Devices.data
	}
}

# Get all devices from Autotask + locations & contacts for spreadsheet exports
$Autotask_Devices = @()
if ($AutotaskConnected -and $Autotask_ID) {
	$Autotask_Devices = Get-AutotaskAPIResource -Resource ConfigurationItems -SimpleSearch "companyID eq $Autotask_ID"
	$Autotask_Devices = $Autotask_Devices | Where-Object { $_.isActive -eq "True" }
	$Autotask_Locations = Get-AutotaskAPIResource -Resource CompanyLocations -SimpleSearch "companyID eq $Autotask_ID"
	$Autotask_Locations = $Autotask_Locations | Where-Object { $_.isActive -eq "True" }
	$Autotask_Contacts = Get-AutotaskAPIResource -Resource Contacts -SimpleSearch "companyID eq $Autotask_ID"
	$Autotask_Contacts = $Autotask_Contacts | Where-Object { $_.isActive -eq 1 }
}

# Get all devices from SC

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
$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/AuthenticationService.ashx/TryLogin" -SessionVariable 'SCWebSession' -Body $FormBody -Method 'POST' -ContentType 'application/json'

# Download the full device list report and then import it
$Response = Invoke-WebRequest "$($SCLogin.URL)/Report.csv?ReportType=Session&SelectFields=SessionID&SelectFields=Name&SelectFields=GuestMachineName&SelectFields=GuestMachineSerialNumber&SelectFields=GuestHardwareNetworkAddress&SelectFields=GuestOperatingSystemName&SelectFields=GuestLastActivityTime&SelectFields=GuestInfoUpdateTime&SelectFields=GuestLastBootTime&SelectFields=GuestLoggedOnUserName&SelectFields=GuestLoggedOnUserDomain&SelectFields=GuestMachineManufacturerName&SelectFields=GuestMachineModel&SelectFields=GuestMachineDescription&SelectFields=CustomProperty1&Filter=SessionType%20%3D%20'Access'%20AND%20NOT%20IsEnded&AggregateFilter=&ItemLimit=100000" -WebSession $SCWebSession
$SC_Devices = $Response.Content | ConvertFrom-Csv

# Function to convert imported UTC date/times to local time for easier comparisons
function Convert-UTCtoLocal {
	param( [parameter(Mandatory=$true)] [String] $UTCTime )
	$strCurrentTimeZone = (Get-WmiObject win32_timezone).StandardName 
	$TZ = [System.TimeZoneInfo]::FindSystemTimeZoneById($strCurrentTimeZone) 
	$LocalTime = [System.TimeZoneInfo]::ConvertTimeFromUtc($UTCTime, $TZ)
	return $LocalTime
}

############
# Connect to the Sophos API to get the device list from Sophos
############

# Get token
$SophosTenantID = $false
$Body = @{
	grant_type = "client_credentials"
	client_id = $SophosAPIKey.ClientID
	client_secret = $SophosAPIKey.Secret
	scope = "token"
}
$SophosToken = Invoke-RestMethod -Method POST -Body $Body -ContentType "application/x-www-form-urlencoded" -uri "https://id.sophos.com/api/v2/oauth2/token"
$SophosJWT = $SophosToken.access_token

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
		}

		# Get the tenants ID and URL
		if ($SophosTenants.items -and $Sophos_Company) {
			$CompanyInfo = $SophosTenants.items | Where-Object { $_.name -like $Sophos_Company }
			$SophosTenantID = $CompanyInfo.id
			$TenantApiHost = $CompanyInfo.apiHost
		}
	}
}

# Finally get the Sophos endpoints
$SophosEndpoints = $false
if ($SophosTenantID -and $TenantApiHost) {
	$SophosHeader = @{
		Authorization = "Bearer $SophosJWT"
		"X-Tenant-ID" = $SophosTenantID
	}
	$SophosEndpoints = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri ($TenantApiHost + "/endpoint/v1/endpoints?pageSize=500")

	$NextKey = $false
	if ($SophosEndpoints.pages.nextKey) {
		$NextKey = $SophosEndpoints.pages.nextKey
	}
	while ($NextKey) {
		$SophosEndpoints_NextPage = $false
		$SophosEndpoints_NextPage = Invoke-RestMethod -Method GET -Headers $SophosHeader -uri ($TenantApiHost + "/endpoint/v1/endpoints?pageFromKey=$NextKey")
		$SophosEndpoints.items += $SophosEndpoints_NextPage.items

		$NextKey = $false
		if ($SophosEndpoints_NextPage.pages.nextKey) {
			$NextKey = $SophosEndpoints_NextPage.pages.nextKey
		}
	}
}

if ($SophosEndpoints -and $SophosEndpoints.items) {
	$Sophos_Devices = $SophosEndpoints.items | Where-Object { $_.type -eq "computer" -or $_.type -eq "server" }
} else {
	$Sophos_Devices = @()
	Write-Host "Warning! Could not get device list from Sophos!" -ForegroundColor Red
}

############
# End Sophos device collection
###########


# Get RMM device details if using the API
if ($RMM_ID) {
	$i = 0
	foreach ($Device in $RMM_Devices) {
		$i++
		[int]$PercentComplete = ($i / $RMM_Devices.count * 100)
		Write-Progress -Activity "Getting RMM device details" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "%")
		$Device | Add-Member -NotePropertyName serialNumber -NotePropertyValue $false
		$Device | Add-Member -NotePropertyName manufacturer -NotePropertyValue $false
		$Device | Add-Member -NotePropertyName model -NotePropertyValue $false
		$Device | Add-Member -NotePropertyName MacAddresses -NotePropertyValue @()

		$AuditDevice = Get-DrmmAuditDevice $Device.uid
		if ($AuditDevice) {
			$Device.serialNumber = $AuditDevice.bios.serialNumber
			$Device.manufacturer = $AuditDevice.systemInfo.manufacturer
			$Device.model = $AuditDevice.systemInfo.model
			$Device.MacAddresses = @($AuditDevice.nics | Select-Object instance, macAddress)
		}
	}
	Write-Progress -Activity "Getting RMM device details" -Status "Ready" -Completed
}

Write-Host "Imported all devices."
Write-Host "===================="

# Filter Screen Connect Devices
if ($SC_Company.GetType().Name -like "String") {
	$SC_Devices = $SC_Devices | Where-Object { $_.CustomProperty1 -like $SC_Company }
} else {
	$SC_Devices_Temp = @()
	foreach ($Company in $SC_Company) {
		$SC_Devices_Temp += $SC_Devices | Where-Object { $_.CustomProperty1 -like $Company }
	}
	$SC_Devices = $SC_Devices_Temp
}

# Filter columns to only the one's we want
$SC_Devices = $SC_Devices | Select-Object SessionID, Name, GuestMachineName, GuestMachineSerialNumber, GuestHardwareNetworkAddress, 
											@{Name="DeviceType"; E={if ($_.GuestOperatingSystemName -like "*Server*") { "Server" } else { "Workstation" } }}, 
											@{Name="GuestLastActivityTime"; E={Convert-UTCtoLocal($_.GuestLastActivityTime)}}, @{Name="GuestInfoUpdateTime"; E={Convert-UTCtoLocal($_.GuestInfoUpdateTime)}}, @{Name="GuestLastBootTime"; E={Convert-UTCtoLocal($_.GuestLastBootTime)}},
											GuestLoggedOnUserName, GuestLoggedOnUserDomain, GuestOperatingSystemName, GuestMachineManufacturerName, GuestMachineModel, GuestMachineDescription
# Sometimes the LastActivityTime field is not set even though the device is on, in these cases it's set to Year 1 
# Also, if a computer is online but inactive, the infoupdate time can be more recent and a better option
# We'll create a new GuestLastSeen property here that is the most recent date of the 3 available
$SC_Devices | Add-Member -NotePropertyName GuestLastSeen -NotePropertyValue $null
$SC_Devices | ForEach-Object { 
	$MostRecentDate = @($_.GuestLastActivityTime, $_.GuestInfoUpdateTime, $_.GuestLastBootTime) | Sort-Object | Select-Object -Last 1
	$_.GuestLastSeen = $MostRecentDate
}

if (!$RMM_ID) {											
	$RMM_Devices = $RMM_Devices | Where-Object { $_."Device Type" -in @("Laptop", "Desktop", "Server") } |
							Select-Object "Device UID", "Device Hostname", "Serial Number", 
											@{Name="MacAddresses"; E={ @(($_."MAC Address(es)" -replace "^\[", '' -replace "\]$", '' -split ', ') | ForEach-Object { @{macAddress = $_} }) }}, 
											"Device Type", "Status", "Last Seen", @{Name="extIpAddress"; E={$_."Ext IP Addr"}}, @{Name="intIpAddress"; E={$_."Int IP Address"}},
											"Last User", Domain, "Operating System", Manufacturer, "Device Model", @{Name="Warranty Expiry"; E= {$_."Warranty Exp. Date"}}, "Device Description", 
											@{Name="ScreenConnectID"; E={
												if ($_.ScreenConnect -and $_.ScreenConnect -like "*$($SCLogin.URL.TrimStart('http').TrimStart('s').TrimStart('://'))*") {
													$Found = $_.ScreenConnect -match '\/\/((\{){0,1}[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\}){0,1})\/Join'
													if ($Found -and $Matches[1]) {
														$Matches[1]
													}
												}
											}}, @{Name="ToDelete"; E={$false}}, @{Name="suspended"; E={"False"}}

} else {
	$RMM_Devices = $RMM_Devices |
							Select-Object @{Name="Device UID"; E={$_.uid}}, @{Name="Device Hostname"; E={$_.hostname}}, @{Name="Serial Number"; E={$_.serialNumber}}, MacAddresses, 
											@{Name="Device Type"; E={$_.deviceType.category}}, @{Name="Status"; E={$_.online}}, @{Name="Last Seen"; E={ if ($_.online -eq "True") { Get-Date } else { Convert-UTCtoLocal(([datetime]'1/1/1970').AddMilliseconds($_.lastSeen)) } }}, 
											extIpAddress, intIpAddress,
											@{Name="Last User"; E={$_.lastLoggedInUser}}, Domain, @{Name="Operating System"; E={$_.operatingSystem}}, 
											Manufacturer, @{Name="Device Model"; E={$_.model}}, @{Name="Warranty Expiry"; E={$_.warrantyDate}}, @{Name="Device Description"; E={$_.description}}, 
											@{Name="ScreenConnectID"; E={
												$SC = $_.udf.udf13;
												if ($SC -and $SC -like "*$($SCLogin.URL.TrimStart('http').TrimStart('s').TrimStart('://'))*") {
													$Found = $SC -match '\/\/((\{){0,1}[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\}){0,1})\/Join'
													if ($Found -and $Matches[1]) {
														$Matches[1]
													}
												}
											}}, @{Name="ToDelete"; E={ if ($_.udf.udf30 -eq "True") { $true } else { $false } }}, suspended
}
$Sophos_Devices = $Sophos_Devices | Select-Object id, @{Name="hostname"; E={$_.hostname -replace '[^\x00-\x7F]+', ''}}, macAddresses, 
										@{Name="type"; E={if ($_.type -eq "computer") { "Workstation"} else { "Server" }}}, 
										lastSeenAt, @{Name="LastUser"; E={($_.associatedPerson.viaLogin -split '\\')[1]}}, @{Name="OS"; E={if ($_.os.name) { $_.os.name } else { "$($_.os.platform) $($_.os.majorVersion).$($_.os.minorVersion)" }}}

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

foreach ($Device in $Sophos_Devices) {
	$Device | Add-Member -NotePropertyName webID -NotePropertyValue $false
	$EndpointID = $Device.id
	$WebEndpointID = convert_sophos_id_to_web $EndpointID
	$Device.webID = $WebEndpointID
}

##############
# Matching Section
##############

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

# Match devices between the device lists
Write-Host "Matching devices..."
$MatchedDevices = @()

# Match ScreenConnect devices with themselves to find any duplicates
foreach ($Device in $SC_Devices) {

	if ($Device.SessionID -in $MatchedDevices.sc_matches) {
		continue
	}

	$Related_SCDevices = @()
	$ForcedMatches = force_match -DeviceID $Device.SessionID -Type "SC"
	$ForcedMatches = $ForcedMatches | Where-Object { $_.type -like 'sc' }

	# Check for force matches first
	if ($ForcedMatches) {
		# Match IDs
		foreach ($Match in ($ForcedMatches | Where-Object { $_.id -ne $false })) {
			$Related_SCDevices += @($SC_Devices | Where-Object { $_.SessionID -like $Match.id })
		}
		# If $ForcedMatches contains the id = $false, stop checking for duplicates
		if ((($ForcedMatches | Where-Object { $_.id -eq $false }) | Measure-Object).Count -gt 0) {
			Continue
		}
	}

	$Related_SCDevices += @($SC_Devices | Where-Object { 
		$Device.SessionID -notlike $_.SessionID -and (
			($Device.GuestMachineSerialNumber.Trim() -and $Device.GuestMachineSerialNumber -notin $IgnoreSerials -and $Device.GuestMachineSerialNumber -notlike "123456789*" -and $_.GuestMachineSerialNumber -like $Device.GuestMachineSerialNumber) -or
			($Device.Name.Trim() -and ($_.Name -eq $Device.Name -or $_.GuestMachineName -eq $Device.Name) -and ($_.GuestMachineSerialNumber -like $Device.GuestMachineSerialNumber -or $_.GuestHardwareNetworkAddress -eq $Device.GuestHardwareNetworkAddress)) -or
			($Device.GuestMachineName.Trim() -and ($_.GuestMachineName -eq $Device.GuestMachineName -or $_.Name -eq $Device.GuestMachineName) -and ($_.GuestMachineSerialNumber -like $Device.GuestMachineSerialNumber -or $_.GuestHardwareNetworkAddress -eq $Device.GuestHardwareNetworkAddress))
		)
	})

	# Get mac address matches only separately, then see if we can cross-reference them with RMM and ignore any that are from USB network adapters
	$MacRelated_SCDevices = @($SC_Devices | Where-Object { 
		$Device.SessionID -notlike $_.SessionID -and (
			($Device.GuestHardwareNetworkAddress -and $_.GuestHardwareNetworkAddress -eq $Device.GuestHardwareNetworkAddress -and $Device.GuestMachineModel -notlike "Virtual Machine") 
		)
	})
	$MacRelated_SCDevices = $MacRelated_SCDevices | Where-Object {
		$Related_RMMDeviceMacs = $RMM_Devices.MacAddresses | Where-Object { $_.macAddress -like $Device.GuestHardwareNetworkAddress }
		if (($Related_RMMDeviceMacs | Measure-Object).Count -gt 0 -and $Related_RMMDeviceMacs.instance -notlike "*USB*" -and $Related_RMMDeviceMacs.instance -notlike "*Ethernet Adapter*") {
			$_
			return
		}
	}
	if (($MacRelated_SCDevices | Measure-Object).Count -gt 0) {
		$Related_SCDevices += $MacRelated_SCDevices
	}
	$Related_SCDevices = @($Related_SCDevices | Sort-Object SessionID -Unique)


	if (($Related_SCDevices | Measure-Object).Count -gt 0) {
		$Related_SCDevices += $Device
		$MatchedDevices += [PsCustomObject]@{
			id = New-Guid
			sc_matches = @($Related_SCDevices.SessionID)
			sc_hostname = @($Related_SCDevices.Name)
			rmm_matches = @()
			rmm_hostname = @()
			sophos_matches = @()
			sophos_hostname = @()
			itg_matches = @()
			itg_hostname = @()
			autotask_matches = @()
			autotask_hostname = @()
		}

	} else {
		$MatchedDevices += [PsCustomObject]@{
			id = New-Guid
			sc_matches = @($Device.SessionID)
			sc_hostname = @($Device.Name)
			rmm_matches = @()
			rmm_hostname = @()
			sophos_matches = @()
			sophos_hostname = @()
			itg_matches = @()
			itg_hostname = @()
			autotask_matches = @()
			autotask_hostname = @()
		}
	}
}

# Match ScreenConnect devices to RMM
foreach ($MatchedDevice in $MatchedDevices) {
	$Matched_SC_Devices = $SC_Devices | Where-Object { $_.SessionID -in $MatchedDevice.sc_matches }

	foreach ($Device in $Matched_SC_Devices) {
		$Related_RMMDevices = @()

		$ForcedMatches = force_match -DeviceID $Device.SessionID -Type "SC"
		$ForcedMatches = $ForcedMatches | Where-Object { $_.type -like 'rmm' }
		$IgnoreRMM = @(($ForcedMatches | Where-Object { $_.match -eq $false }).id)
		$ForcedMatches = @($ForcedMatches | Where-Object { $_.id -ne $false -and $_.match -ne $false })

		while (!$Related_RMMDevices) {
			# Forced matches
			if ($ForcedMatches) {
				# Match IDs
				foreach ($Match in $ForcedMatches) {
					$Related_RMMDevices += @($RMM_Devices | Where-Object { $_."Device UID" -like $Match.id })
				}
			}
			# If $IgnoreRMM contains $False, stop here as that means we dont want to match with any more rmm devices
			if ($IgnoreRMM -contains $false) {
				break;
			}
			# Remove false entries in $IgnoreRMM now so we can easily check against the list of IDs
			$IgnoreRMM = @($IgnoreRMM | Where-Object { $_ })
			# Screen connect session ID
			$Related_RMMDevices += $RMM_Devices | Where-Object { $_.ScreenConnectID -like $Device.SessionID -and $_."Device UID" -notin $IgnoreRMM }
			# Serial number
			if ($Device.GuestMachineSerialNumber.Trim() -and $Device.GuestMachineSerialNumber -notin $IgnoreSerials -and $Device.GuestMachineSerialNumber -notlike "123456789*") {
				$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Serial Number" -like $Device.GuestMachineSerialNumber -and $_."Device UID" -notin $IgnoreRMM }
			}
			# Hostname
			if ($Device.Name.Trim()) {
				$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Device Hostname" -eq $Device.Name -and $_."Device UID" -notin $IgnoreRMM }
			}
			if ($Device.GuestMachineName.Trim()) {
				$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Device Hostname" -eq $Device.GuestMachineName -and $_."Device UID" -notin $IgnoreRMM }
			}
			# Mac address  (if this is a VM, only check this if we haven't found any related devices so far. VM's can cause false positives with this search.)
			if ($Device.GuestHardwareNetworkAddress -and (!$Related_RMMDevices -or $Device.GuestMachineModel -notlike "Virtual Machine")) {
				$MacRelated_RMMDevices = $RMM_Devices | Where-Object { $_.MacAddresses.macAddress -contains $Device.GuestHardwareNetworkAddress -and $_."Device UID" -notin $IgnoreRMM }
				if ($MacRelated_RMMDevices.MacAddresses.instance) {
					$MacRelated_RMMDevices = $MacRelated_RMMDevices | Where-Object { 
						# Remove any usb adapter mac matches unless the hostname also matches
						$ConnectedMac = $_.MacAddresses | Where-Object { $_.macAddress -like $Device.GuestHardwareNetworkAddress }
						if (($ConnectedMac.instance -like "*USB*" -or $ConnectedMac.instance -like "*Ethernet Adapter*") -and $Device.Name -notlike $_."Device Hostname" -and $Device.GuestMachineName -notlike $_."Device Hostname") {
							$false
							return
						} else {
							$_
							return
						}
					}
				}
				$Related_RMMDevices += $MacRelated_RMMDevices
			}

			# Description searches as a backup
			if (!$Related_RMMDevices) {
				if ($Device.Name.Trim()) {
					$EscapedName = $Device.Name.replace("[", "````[").replace("]", "````]")
					$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Device Description" -like "*$($EscapedName)*" -and $_."Device UID" -notin $IgnoreRMM }
				}
				if ($Device.GuestMachineName.Trim()) {
					$EscapedName2 = $Device.GuestMachineName.replace("[", "````[").replace("]", "````]")
					$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Device Description" -like "*$($EscapedName2)*" -and $_."Device UID" -notin $IgnoreRMM }
				}
			}
			if (!$Related_RMMDevices -and $Device.GuestMachineDescription.Trim() -and $Device.GuestMachineDescription.length -gt 5 -and $Device.GuestMachineDescription -like "*-*" -and $Device.GuestMachineDescription.Trim() -notlike "* *") {
				$Related_RMMDevices += $RMM_Devices | Where-Object { $_."Device Description" -like "*$($Device.GuestMachineDescription.Trim())*" -and $_."Device UID" -notin $IgnoreRMM }
			}
			
			$Related_RMMDevices = $Related_RMMDevices | Sort-Object "Device UID" -Unique

			break;
		}

		# If there was more than 1 related device found, try to filter the results down (particularly matches found on hostname)
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$_.ScreenConnectID -like $Device.SessionID -or
				($Device.GuestMachineSerialNumber.Trim() -and $Device.GuestMachineSerialNumber -notin $IgnoreSerials -and $Device.GuestMachineSerialNumber -notlike "123456789*" -and $_."Serial Number" -like $Device.GuestMachineSerialNumber) -or
				($Device.GuestHardwareNetworkAddress -and $_.MacAddresses.macAddress -contains $Device.GuestHardwareNetworkAddress -and $Device.GuestMachineModel -notlike "Virtual Machine") -or
				($Device.Name.Trim() -and $_."Device Hostname" -eq $Device.Name -and ($_."Serial Number" -like $Device.GuestMachineSerialNumber -or $_.MacAddresses.macAddress -contains $Device.GuestHardwareNetworkAddress)) -or
				($Device.GuestMachineName.Trim() -and $_."Device Hostname" -eq $Device.GuestMachineName -and ($_."Serial Number" -like $Device.GuestMachineSerialNumber -or $_.MacAddresses.macAddress -contains $Device.GuestHardwareNetworkAddress)) -or
				$_."Device UID" -in $ForcedMatches.id
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 1) {
				# If there is still more than 1 match, try removing any matches based on a USB network adapters mac address (but still keep them if the hostname matches)
				$Related_RMMDevices_Filtered = $Related_RMMDevices_Filtered | Where-Object { $_."Device Hostname" -eq $Device.Name -or $_."Device Hostname" -eq $Device.GuestMachineName -or $_.MacAddresses.macAddress -notlike $Device.GuestHardwareNetworkAddress -or ($_.MacAddresses.macAddress -like $Device.GuestHardwareNetworkAddress -and $_.MacAddresses.instance -notlike "*USB*" -and $_.MacAddresses.instance -notlike "*Ethernet Adapter*") }
				if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
					$Related_RMMDevices = $Related_RMMDevices_Filtered
				}
			}
		}

		# If we found related devices, add them to the matched device list
		if (($Related_RMMDevices | Measure-Object).Count -gt 0) {
			$MatchedDevice.rmm_matches = @($Related_RMMDevices."Device UID")
			$MatchedDevice.rmm_hostname = @($Related_RMMDevices."Device Hostname")
		}
	}
}

# Add any missing RMM devices that no matches were found for
foreach ($Device in $RMM_Devices) {
	if ($Device."Device UID" -in $MatchedDevices.rmm_matches) {
		continue
	}

	$MatchedDevices += [PsCustomObject]@{
		id = New-Guid
		sc_matches = @()
		sc_hostname = @()
		rmm_matches = @($Device."Device UID")
		rmm_hostname = @($Device."Device Hostname")
		sophos_matches = @()
		sophos_hostname = @()
		itg_matches = @()
		itg_hostname = @()
		autotask_matches = @()
		autotask_hostname = @()
	}
}

# Match Sophos devices
foreach ($Device in $Sophos_Devices) {
	$CleanDeviceName = $Device.hostname -replace '\W', ''
	$RelatedDevices = @()
	$AddedForcedMatches = @()

	$ForcedMatches = force_match -DeviceID $Device.id -Type "Sophos"
	$ForcedMatches = $ForcedMatches | Where-Object { $_.type -like 'sc' -or $_.type -like 'rmm' }
	$IgnoreSC = @(($ForcedMatches | Where-Object { $_.type -like 'sc' -and $_.match -eq $false }).id)
	$IgnoreRMM = @(($ForcedMatches | Where-Object { $_.type -like 'rmm' -and $_.match -eq $false }).id)
	$ForcedMatches = @($ForcedMatches | Where-Object { $_.id -ne $false -and $_.match -ne $false })

	# Forced matches
	if ($ForcedMatches) {
		# Match IDs
		foreach ($Match in $ForcedMatches) {
			if ($Match.type -like 'rmm') {
				$RMMMatches = @($MatchedDevices | Where-Object { $_.rmm_matches -contains $Match.id })
				$RelatedDevices += $RMMMatches
				$AddedForcedMatches += $RMMMatches.id
			} elseif ($Match.type -like 'sc') {
				$SCMatches = @($MatchedDevices | Where-Object { $_.sc_matches -contains $Match.id })
				$RelatedDevices += $SCMatches
				$AddedForcedMatches += $SCMatches.id
			}
		}
	}
	# If $IgnoreRMM and $IgnoreSC both contain $False, stop here as that means we dont want to match with any more devices. Create an empty entry first if we havent force matched any devices already.
	if ($IgnoreRMM -contains $false -and $IgnoreSC -contains $false) {
		if (!$DidForceMatches) {
			$MatchedDevices += [PsCustomObject]@{
				id = New-Guid
				sc_matches = @()
				sc_hostname = @()
				rmm_matches = @()
				rmm_hostname = @()
				sophos_matches = @($Device.id)
				sophos_hostname = @($Device.hostname)
				itg_matches = @()
				itg_hostname = @()
				autotask_matches = @()
				autotask_hostname = @()
			}
		}
		continue;
	}

	# Sophos to SC and RMM Matches
	# If the device name is more than 15 characters, do a partial search as Sophos will get the full computer name, but SC and RMM will get the hostname (which is limited to 15 characters)

	# Sophos to SC Matches
	if ($IgnoreSC -notcontains $false -and ($Device.hostname -in $MatchedDevices.sc_hostname -or $CleanDeviceName -in ($MatchedDevices.sc_hostname -replace '\W', '') -or 
		($Device.hostname.length -gt 15 -and (($MatchedDevices.sc_hostname | Where-Object { $Device.hostname -like "$([Management.Automation.WildcardPattern]::Escape($_))*" }).count -gt 0))))
	{
		$RelatedDevices += ($MatchedDevices | Where-Object { 
			($Device.hostname -in $_.sc_hostname -or $CleanDeviceName -in ($_.sc_hostname -replace '\W', '')) -and 
			(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
			(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
			($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
		})
		# Try a partial hostname match for long hostnames (where they might get cutoff)
		if ($Device.hostname.length -gt 15) {
			$RelatedDevices += ($MatchedDevices | Where-Object { 
				($_.sc_hostname | Where-Object { $Device.hostname -like "$([Management.Automation.WildcardPattern]::Escape($_))*" }).count -gt 0 -and 
				(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
			(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
				($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
			})
		}
	}

	# Sophos to RMM Matches
	if ($IgnoreRMM -notcontains $false -and ($Device.hostname -in $MatchedDevices.rmm_hostname -or $CleanDeviceName -in ($MatchedDevices.rmm_hostname -replace '\W', '') -or
		($Device.hostname.length -gt 15 -and ($MatchedDevices.rmm_hostname | Where-Object { $Device.hostname -like "$([Management.Automation.WildcardPattern]::Escape($_))*" }).count -gt 0))) 
	{
		$RelatedDevices += ($MatchedDevices | Where-Object { 
			($Device.hostname -in $_.rmm_hostname -or $CleanDeviceName -in ($_.rmm_hostname -replace '\W', '')) -and 
			(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
			(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
			($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
		})
		# Try a partial hostname match for long hostnames (where they might get cutoff)
		if (!$RelatedDevices -and $Device.hostname.length -gt 15) {
			$RelatedDevices += ($MatchedDevices | Where-Object { 
				($_.rmm_hostname | Where-Object { $Device.hostname -like "$([Management.Automation.WildcardPattern]::Escape($_))*" }).count -gt 0 -and 
				(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
			(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
				($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
			})
		}
	}

	# If multiple related devices, narrow down to those that match on Mac Address as well and keep forced matches
	if ($Device.macAddresses -and $RelatedDevices -and ($RelatedDevices | Measure-Object).Count -gt 1) {
		$RMMMacMatches = $RMM_Devices | Where-Object { $_.MacAddresses.macAddress | Where-Object { $Device.macAddresses -contains $_ } } 
		$SCMacMatches = $SC_Devices | Where-Object { $Device.macAddresses -contains $_.GuestHardwareNetworkAddress }

		$RelatedDevices_MACMatch = @($RelatedDevices | Where-Object { 
			$RelatedDevice = $_;
			($RelatedDevice.sc_matches | Where-Object { $SCMacMatches.SessionID -contains $_ }) -or 
			($RelatedDevice.rmm_matches | Where-Object { $RMMMacMatches."Device UID" -contains $_ }) 
		})

		if (($RelatedDevices_MACMatch | Measure-Object).count -gt 0) {
			$RelatedDevices_MACMatch += @($RelatedDevices | Where-Object {$_.id -in $AddedForcedMatches }) # Add forced matches in
			$RelatedDevices = $RelatedDevices_MACMatch
		}
	}

	# If we could not match the hostname and this is a macOS devices, lets try matching on MAC address (as MacOS tend to be a bit more difficult to match on hostname)
	if (!$RelatedDevices -and $Device.OS -like "*macOS*" -and $Device.macAddresses -and (($Device.macAddresses | Where-Object { $RMM_Devices.MacAddresses.macAddress -contains $_ }) -or ($Device.macAddresses | Where-Object { $SC_Devices.GuestHardwareNetworkAddress -contains $_ }))) {
		$RMMMacMatches = $RMM_Devices | Where-Object { $_.MacAddresses | Where-Object { $Device.macAddresses -contains $_.macAddress } } 
		$SCMacMatches = $SC_Devices | Where-Object { $Device.macAddresses -contains $_.GuestHardwareNetworkAddress }

		$RelatedDevices += $MatchedDevices | Where-Object { 
			$RelatedDevice = $_;
			(($RelatedDevice.sc_matches | Where-Object { $SCMacMatches.SessionID -contains $_ }) -or 
			($RelatedDevice.rmm_matches | Where-Object { $RMMMacMatches."Device UID" -contains $_ })) -and
			(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
			(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
			($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
		}
	}

	$RelatedDevices = $RelatedDevices | Sort-Object id -Unique

	# Got all related devices, updated $MatchedDevices
	if (($RelatedDevices | Measure-Object).Count -gt 0) {
		foreach ($MatchedDevice in $RelatedDevices) {
			$MatchedDevice.sophos_matches += @($Device.id)
			$MatchedDevice.sophos_hostname += @($Device.hostname)
		}
	} else {
		# No related devices found, try checking RMM descriptions (for manual matches)
		$Related_RMMDevices = $RMM_Devices | Where-Object { $_."Device Description" -like "*$($Device.hostname)*" -or $_."Device Description" -like "*$($CleanDeviceName)*" -or ($_."Device Description" -replace '\W', '') -like "*$($CleanDeviceName)*" }
		$Related_RMMDevices = $Related_RMMDevices | Sort-Object "Device UID" -Unique

		$RelatedDevices = @()
		if (($Related_RMMDevices | Measure-Object).Count -gt 0) {
			foreach ($RMMDevice in $Related_RMMDevices) {
				$RelatedDevices += ($MatchedDevices | Where-Object { 
					$RMMDevice."Device UID" -in $_.rmm_matches -and
					(!$_.sc_matches -or !$IgnoreSC -or ($_.sc_matches | Where-Object { $_ -notin $IgnoreSC })) -and 
					(!$_.rmm_matches -or !$IgnoreRMM -or ($_.rmm_matches | Where-Object { $_ -notin $IgnoreRMM })) -and
					($IgnoreRMM -notcontains $false -or !$_.rmm_matches) -and ($IgnoreSC -notcontains $false -or !$_.sc_matches)
				})
			}
		}

		if (($RelatedDevices | Measure-Object).Count -gt 0) {
			foreach ($MatchedDevice in $RelatedDevices) {
				$MatchedDevice.sophos_matches += @($Device.id)
				$MatchedDevice.sophos_hostname += @($Device.hostname)
			}
		} else {

			# Still no related devices found, add an empty match
			$MatchedDevices += [PsCustomObject]@{
				id = New-Guid
				sc_matches = @()
				sc_hostname = @()
				rmm_matches = @()
				rmm_hostname = @()
				sophos_matches = @($Device.id)
				sophos_hostname = @($Device.hostname)
				itg_matches = @()
				itg_hostname = @()
				autotask_matches = @()
				autotask_hostname = @()
			}
		}
	}
}

# Match devices to ITG
if ($ITGConnected) {
	foreach ($Device in $ITG_Devices) {
		$RelatedDevices = @()

		# ITG to RMM Matches
		$Related_RMMDevices = @()
		$Related_RMMDevices += ($RMM_Devices | Where-Object {
			$Device.attributes.'asset-tag' -eq $_.'Device UID' -or 
			$Device.attributes.name -like $_.'Device Hostname' -or 
			($Device.attributes.hostname -like $_.'Device Hostname' -and $Device.attributes.hostname) -or 
			$Device.attributes.name -like $_.'Device Description' -or 
			($Device.attributes.hostname -like $_.'Device Description' -and $Device.attributes.hostname) -or 
			($Device.attributes.'serial-number' -like $_.'Serial Number' -and $Device.attributes.'serial-number' -and $Device.attributes.'serial-number' -notin $IgnoreSerials -and $Device.attributes.'serial-number' -notlike "123456789*")
		})

		# Narrow down if more than 1 device found
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$Device.attributes.name -like $_.'Device Hostname' -and
				$Device.attributes.'serial-number' -like $_.'Serial Number'
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
		}
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$Device.attributes.'asset-tag' -eq $_.'Device UID'
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
		}

		# Get existing matches and connect
		$Related_RMMDevices | ForEach-Object {
			$RMM_DeviceID = $_."Device UID"
			$RelatedDevices += ($MatchedDevices | Where-Object { $RMM_DeviceID -in $_.rmm_matches })
		}


		# ITG to SC Matches (fallback)
		if (!$RelatedDevices) {
			$Related_SCDevices = @()
			$Related_SCDevices += ($SC_Devices | Where-Object {
				$Device.attributes.name -like $_.Name -or 
				($Device.attributes.hostname -like $_.Name -and $Device.attributes.hostname) -or 
				$Device.attributes.name -like $_.GuestMachineName -or 
				($Device.attributes.hostname -like $_.GuestMachineName -and $Device.attributes.hostname) -or 
				($Device.attributes.'serial-number' -like $_.GuestMachineSerialNumber -and $Device.attributes.'serial-number' -and $Device.attributes.'serial-number' -notin $IgnoreSerials -and $Device.attributes.'serial-number' -notlike "123456789*")
			})

			# Narrow down if more than 1 device found
			if (($Related_SCDevices | Measure-Object).Count -gt 1) {
				$Related_SCDevices_Filtered = $Related_SCDevices | Where-Object { 
					$Device.attributes.'serial-number' -like $_.GuestMachineSerialNumber
				}
				if (($Related_SCDevices_Filtered | Measure-Object).Count -gt 0) {
					$Related_SCDevices = $Related_SCDevices_Filtered
				}
			}

			# Get existing matches and connect
			$Related_SCDevices | ForEach-Object {
				$SC_DeviceID = $_.SessionID
				$RelatedDevices += ($MatchedDevices | Where-Object { $SC_DeviceID -in $_.sc_matches })
			}
		}

		# ITG to Sophos Matches (fallback)
		if (!$RelatedDevices) {
			$Related_SophosDevices = @()
			$Related_SophosDevices += ($Sophos_Devices | Where-Object {
				$Device.attributes.name -eq $_.hostname -or
				($Device.attributes.hostname -eq $_.hostname -and $Device.attributes.hostname)
			})

			# If matching is for MacOS devices and multiple are found, skip matching
			if (($Related_SophosDevices | Measure-Object).Count -gt 1 -and $Related_SophosDevices.OS -like "*macOS*") {
				continue
			}
		}

		$RelatedDevices = $RelatedDevices | Sort-Object id -Unique

		# Got all related devices, updated $MatchedDevices
		if (($RelatedDevices | Measure-Object).Count -gt 0) {
			foreach ($MatchedDevice in $RelatedDevices) {
				$MatchedDevice.itg_matches += @($Device.id)
				$MatchedDevice.itg_hostname += @($Device.attributes.name)
			}
		}
	}
}

# Match devices to Autotask
if ($AutotaskConnected) {
	foreach ($Device in $Autotask_Devices) {
		$RelatedDevices = @()

		# Autotask to RMM Matches
		$Related_RMMDevices = @()
		$Related_RMMDevices += ($RMM_Devices | Where-Object {
			($Device.rmmDeviceUID -eq $_.'Device UID' -and $Device.rmmDeviceUID) -or 
			($Device.referenceNumber -eq $_.'Device UID' -and $Device.referenceNumber) -or 
			$Device.referenceTitle -like $_.'Device Hostname' -or 
			($Device.rmmDeviceAuditHostname -like $_.'Device Hostname' -and $Device.rmmDeviceAuditHostname) -or 
			($Device.serialNumber -like $_.'Serial Number' -and $Device.serialNumber -and $Device.serialNumber -notin $IgnoreSerials -and $Device.serialNumber -notlike "123456789*")
		})

		# Narrow down if more than 1 device found
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$Device.rmmDeviceUID -eq $_.'Device UID'
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
		}
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$Device.referenceTitle -like $_.'Device Hostname' -and
				$Device.serialNumber -like $_.'Serial Number'
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
		}
		if (($Related_RMMDevices | Measure-Object).Count -gt 1) {
			$Related_RMMDevices_Filtered = $Related_RMMDevices | Where-Object { 
				$Device.referenceNumber -eq $_.'Device UID'
			}
			if (($Related_RMMDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_RMMDevices = $Related_RMMDevices_Filtered
			}
		}

		# Get existing matches and connect
		$Related_RMMDevices | ForEach-Object {
			$RMM_DeviceID = $_."Device UID"
			$RelatedDevices += ($MatchedDevices | Where-Object { $RMM_DeviceID -in $_.rmm_matches })
		}

		# Autotask to ITG Matches (fallback)
		$Related_ITGDevices = @()
		$Related_ITGDevices += ($ITG_Devices | Where-Object {
			($Device.rmmDeviceUID -eq $_.attributes.'asset-tag' -and $Device.rmmDeviceUID) -or 
			($Device.referenceNumber -eq $_.attributes.'asset-tag' -and $Device.referenceNumber) -or 
			$Device.referenceTitle -like $_.attributes.name -or 
			$Device.referenceTitle -like $_.attributes.hostname -or 
			($Device.rmmDeviceAuditHostname -like $_.attributes.name -and $Device.rmmDeviceAuditHostname) -or 
			($Device.rmmDeviceAuditHostname -like $_.attributes.hostname -and $Device.rmmDeviceAuditHostname) -or 
			($Device.rmmDeviceAuditDescription -like $_.attributes.name -and $Device.rmmDeviceAuditDescription) -or 
			($Device.rmmDeviceAuditDescription -like $_.attributes.hostname -and $Device.rmmDeviceAuditDescription) -or 
			($Device.serialNumber -like $_.attributes.'serial-number' -and $Device.serialNumber -and $Device.serialNumber -notin $IgnoreSerials -and $Device.serialNumber -notlike "123456789*")
		})

		# Narrow down if more than 1 device found
		if (($Related_ITGDevices | Measure-Object).Count -gt 1) {
			$Related_ITGDevices_Filtered = $Related_ITGDevices | Where-Object { 
				$Device.referenceTitle -like $_.attributes.name -and
				$Device.serialNumber -like $_.attributes.'serial-number'
			}
			if (($Related_ITGDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_ITGDevices = $Related_ITGDevices_Filtered
			}
		}
		if (($Related_ITGDevices | Measure-Object).Count -gt 1) {
			$Related_ITGDevices_Filtered = $Related_ITGDevices | Where-Object { 
				$Device.rmmDeviceUID -eq $_.attributes.'asset-tag' -or
				$Device.referenceNumber -eq $_.attributes.'asset-tag'
			}
			if (($Related_ITGDevices_Filtered | Measure-Object).Count -gt 0) {
				$Related_ITGDevices = $Related_ITGDevices_Filtered
			}
		}

		# Get existing matches and connect
		$Related_ITGDevices | ForEach-Object {
			$ITG_DeviceID = $_.id
			$RelatedDevices += ($MatchedDevices | Where-Object { $ITG_DeviceID -in $_.itg_matches })
		}

		$RelatedDevices = $RelatedDevices | Sort-Object id -Unique

		# Got all related devices, updated $MatchedDevices
		if (($RelatedDevices | Measure-Object).Count -gt 0) {
			foreach ($MatchedDevice in $RelatedDevices) {
				$MatchedDevice.autotask_matches += @($Device.id)
				$MatchedDevice.autotask_hostname += @($Device.referenceTitle)
			}
		}
	}
}

Write-Host "Matching Complete!"
Write-Host "===================="
$MatchedDevices | Select-Object id, @{N="sc_hostname"; E={$_.sc_hostname -join ', '}}, @{N="rmm_hostname"; E={$_.rmm_hostname -join ', '}}, @{N="sophos_hostname"; E={$_.sophos_hostname -join ', '}}, @{N="itg_hostname"; E={$_.itg_hostname -join ', '}}, @{N="autotask_hostname"; E={$_.autotask_hostname -join ', '}} | Out-GridView -PassThru -Title "Matched Devices"

# Get the existing log
$LogFilePath = "$($LogLocation)\$($Company_Acronym)_log.json"
if ($LogLocation -and (Test-Path -Path $LogFilePath)) {
	$LogHistory = Get-Content -Path $LogFilePath -Raw | ConvertFrom-Json
} else {
	$LogHistory = @{}
}

# Function for logging automated changes (installs, deletions, etc.)
# $ServiceTarget is 'rmm', 'sc', 'sophos', 'itg', or 'autotask'
function log_change($Company_Acronym, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $ChangeType, $Hostname = "", $Reason = "") {
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

# Checks the log history to see if something has been attempted more than 5 times
# and attempts have been made for the past 2 weeks
# If so, an email is sent if one hasn't already been sent in the last 2 weeks, and the email is logged
# $ErrorMessage can use HTML and it will become the main body of the email sent
function check_failed_attempts($LogHistory, $Company_Acronym, $ErrorMessage, $ServiceTarget, $RMM_Device_ID, $SC_Device_ID, $Sophos_Device_ID, $ChangeType, $Hostname = "", $Reason = "") {
	$TwoWeeksAgo = [int](New-TimeSpan -Start (Get-Date "01/01/1970") -End (Get-Date).AddDays(-14).ToUniversalTime()).TotalSeconds
	$TenDays = [int](New-TimeSpan -Start (Get-Date).AddDays(-10).ToUniversalTime() -End (Get-Date).ToUniversalTime()).TotalSeconds

	# first check if an email was sent about this in the last 2 weeks
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
		$EmailLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMM_Device_ID)"
	} elseif ($ServiceTarget -eq 'sc') {
		$ID_Params."SC_Device_ID" = $SC_Device_ID
		$SCDevice = $SC_Devices | Where-Object { $_.SessionID -eq $SC_Device_ID }
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
function compare_activity_sc($DeviceIDs) {
	$UnixDateLowLimit = Get-Date -Year 1970 -Month 1 -Day 1
	$SCDevices = $SC_Devices | Where-Object { $_.SessionID -in $DeviceIDs }
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
	$Now = Get-Date
	$RMMDevices = $RMM_Devices | Where-Object { $_."Device UID" -in $DeviceIDs }
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
	$SophosDevices = $Sophos_Devices | Where-Object { $_.id -in $DeviceIDs }
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

# Helper function that takes a $MatchedDevices object and returns the activity comparison for SC, RMM, and Sophos
function compare_activity($MatchedDevice) {
	$Activity = @{}

	if ($MatchedDevice.sc_matches -and ($MatchedDevice.sc_matches | Measure-Object).count -gt 0) {
		$SCActivity = compare_activity_sc $MatchedDevice.sc_matches
		$Activity.sc = $SCActivity
	}

	if ($MatchedDevice.rmm_matches -and ($MatchedDevice.rmm_matches | Measure-Object).count -gt 0) {
		$RMMActivity = compare_activity_rmm $MatchedDevice.rmm_matches
		$Activity.rmm = $RMMActivity
	}

	if ($MatchedDevice.sophos_matches -and ($MatchedDevice.sophos_matches | Measure-Object).count -gt 0) {
		$SophosActivity = compare_activity_sophos $MatchedDevice.sophos_matches
		$Activity.sophos = $SophosActivity
	}

	$Activity
	return
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
		$FormBody = '[["All Machines"], ["' + $SC_ID + '"], 21, null]'
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddEventToSessions" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'
		return $true
	} else {
		Write-Warning "Could not get an anti-forgery token from Screenconnect. Failed to delete device from SC: $SC_ID"
		return $false
	}
}

# This doesn't truly delete a device from RMM (we can't using the API), instead it sets the Delete Me UDF which adds the device into the device filter of devices we should delete
function delete_from_rmm($RMM_Device_ID) {
	Set-DrmmDeviceUdf -deviceUid $RMM_Device_ID -udf30 "True"
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
			Write-Error "Could not auto-delete Sophos device '$Sophos_Device_ID' for the reason: " + $_.Exception.Message
		}
	}
	return $false
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
		Write-Error "Could not archive ITG configuration '$ITG_Device_ID' for the reason: " + $_.Exception.Message
		return $false
	}
}


# Find any duplicates that need to be removed
if ($DODuplicateSearch) {
	Write-Host "Searching for duplicates..."
	$Duplicates = @()
	foreach ($Device in $MatchedDevices) {
		if ($Device.sc_matches.count -gt 1 -or $Device.rmm_matches.count -gt 1 <# -or $Device.sophos_matches.count -gt 1 #>) { 
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
					$SCDevice = $SC_Devices | Where-Object { $_.SessionID -eq $OrderedDevice.id }
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
					$RMMDevice = $RMM_Devices | Where-Object { $_."Device UID" -eq $OrderedDevice.id }
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
					$DuplicatesTable += [PsCustomObject]@{
						type = "RMM"
						hostname = $RMMDevice."Device Hostname"
						id = $OrderedDevice.id 
						last_active = $OrderedDevice.last_active
						remove = if ($i -eq 0) { "No" } else { "Yes" }
						auto_deleted = $Deleted
						link = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($OrderedDevice.id)"
					}
					$i++
				}
			}

			# Sophos (Disabled. Sophos really doesn't like you deleting duplicates so best to just leave them)
			<# if ($Device.sophos_matches.count -gt 1) {
				$OrderedDevices = compare_activity_sophos($Device.sophos_matches)
				$i = 0
				foreach ($OrderedDevice in $OrderedDevices) {
					$SophosDevice = $Sophos_Devices | Where-Object { $_.id -eq $OrderedDevice.id }
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
		}

		Write-Host "Warning! Duplicates were found!" -ForegroundColor Red
		[System.Windows.MessageBox]::Show('Duplicate devices were found. Please review.', 'Duplicates Found')
		$DuplicatesTable | Out-GridView -Title "Duplicates Found" -PassThru

		# Now remove duplicates from $MatchedDevices as we can ignore them for the rest of this script
		foreach ($Device in $DuplicatesTable) {
			if ($Device.Remove -eq "No") {
				continue
			}

			if ($Device.type -eq "SC") {
				$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.sc_matches }
				foreach ($MDevice in $MatchedDevice) {
					$MDevice.sc_matches = $MatchedDevice.sc_matches | Where-Object { $_ -ne $Device.id }
					$MDevice.sc_hostname = @(($SC_Devices | Where-Object { $_.SessionID -in $MatchedDevice.sc_matches }).Name)
				}
			}

			if ($Device.type -eq "RMM") {
				$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.rmm_matches }
				foreach ($MDevice in $MatchedDevice) {
					$MDevice.rmm_matches = $MatchedDevice.rmm_matches | Where-Object { $_ -ne $Device.id }
					$MDevice.rmm_hostname = @(($RMM_Devices | Where-Object { $_."Device UID" -in $MatchedDevice.rmm_matches })."Device Hostname")
				}
			}

			if ($Device.type -eq "Sophos") {
				$MatchedDevice = $MatchedDevices | Where-Object { $Device.id -in $_.sophos_matches }
				foreach ($MDevice in $MatchedDevice) {
					$MDevice.sophos_matches = $MatchedDevice.sophos_matches | Where-Object { $_ -ne $Device.id }
					$MDevice.sophos_hostname = @(($Sophos_Devices | Where-Object { $_.id -in $MatchedDevice.sophos_matches }).hostname)
				}
			}
		}
	}

	Write-Host "Duplicate check complete."
	Write-Host "======================"
}

# Check for devices that look like they should not be under this company
if ($DOWrongCompanySearch) {
	Write-Host "Searching for any devices that look like they belong in a different company..."
	$MoveDevices = @()

	foreach ($Device in $MatchedDevices) {
		$Hostnames = @()
		$SCDeviceIDs = @($Device.sc_matches)
		$RMMDeviceIDs = @($Device.rmm_matches)
		$SophosDeviceIDs = @($Device.sophos_matches)

		$DeviceType = $false
		$OperatingSystem = $false

		if ($SCDeviceIDs) {
			$SCDevices = $SC_Devices | Where-Object { $_.SessionID -in $SCDeviceIDs }
			$Hostnames += $SCDevices.Name
			$OperatingSystem = $SCDevices[0].GuestOperatingSystemName
			$DeviceType = $SCDevices[0].DeviceType
		}
		if ($RMMDeviceIDs) {
			$RMMDevices = $RMM_Devices | Where-Object { $_."Device UID" -in $RMMDeviceIDs }
			$Hostnames += $RMMDevices."Device Hostname"
			if (!$OperatingSystem) {
				$OperatingSystem = $RMMDevices[0]."Operating System"
			}
			if ($DeviceType) {
				$DeviceType = $RMMDevices[0]."Device Type"
			}
		}
		if ($SophosDeviceIDs) {
			$SophosDevices = $Sophos_Devices | Where-Object { $_.id -in $SophosDeviceIDs }
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
			$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceIDs[0])"
		}
		if ($SophosDeviceIDs) {
			$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevices[0].webID)"
		}

		# This device does not look like it belongs here, lets flag it
		$MoveDevices += [PsCustomObject]@{
			Hostnames = $Hostnames -join ', '
			DeviceType = $DeviceType
			InSC = if ($SCDeviceIDs) { "Yes" } else { "No" }
			InRMM = if ($RMMDeviceIDs) { "Yes" } else { "No" }
			InSophos = if ($SophosDeviceIDs) { "Yes" } else { "No" }
			SC_Link = $SCLink
			RMM_Link = $RMMLink
			Sophos_Link = $SophosLink
		}
	}

	if (($MoveDevices | Measure-Object).count -gt 0) {
		Write-Host "Warning! Devices found that may not belong in this company!" -ForegroundColor Red

		[System.Windows.MessageBox]::Show("Devices were found that look like they may not belong in this company. Please review. `n Select any devices to remove from the billing report then click 'OK'. To remove none, click cancel.", 'Devices Found with odd Hostnames')
		$RemoveDevices = $MoveDevices | Sort-Object Hostnames,DeviceType | Out-GridView -Title "Devices Found with odd Hostnames" -PassThru
		if (($RemoveDevices | Measure-Object).Count -gt 0) {
			$MatchedDevices = $MatchedDevices | Where-Object { $_.id -notin $RemoveDevices.ID }
			Write-Host "Removed $(($RemoveDevices | Measure-Object).Count) devices." -ForegroundColor Yellow
		}
	}

	Write-Host "Check for devices that don't belong is complete."
	Write-Host "======================"
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

		$FormBody = '[["All Machines"],["' + $SC_ID + '"],44,"' + $RMMInstallCmd + '"]'
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddEventToSessions" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'

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

		$FormBody = '[["All Machines"],["' + $SC_ID + '"],44,"' + $RMMInstallCmd + '"]'
		$Response = Invoke-WebRequest "$($SCLogin.URL)/Services/PageService.ashx/AddEventToSessions" -WebSession $SCWebSession -Headers @{"X-Anti-Forgery-Token" = $AntiForgeryToken} -Body $FormBody -Method 'POST' -ContentType 'application/json'

		return $true
	} else {
		Write-Warning "Could not get an anti-forgery token from Screenconnect. Failed to install RMM for SC device ID: $SC_ID"
		return $false
	}
}

function install_sc_using_rmm($RMM_Device) {
	if ($RMM_Device."Operating System" -like "*Windows*") {
		Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Install ScreenConnect on $($RMM_Device."Device Hostname")" -ComponentName "ScreenConnect Install - WIN"
		return $true
	} elseif ($RMM_Device."Operating System" -like "*Mac OS*") {
		Set-DrmmDeviceQuickJob -DeviceUid $RMM_Device."Device UID" -jobName "Install ScreenConnect on $($RMM_Device."Device Hostname")" -ComponentName "ScreenConnect Install - MAC"
		return $true
	}
	return $false
}

# Find broken device connections (e.g. online recently in ScreenConnect but no in RMM)
if ($DOBrokenConnectionSearch) {
	Write-Host "Searching for broken connections..."
	$BrokenConnections = @()
	foreach ($Device in $MatchedDevices) {
		$ActivityComparison = compare_activity($Device)
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
						$Hostname = ($SC_Devices | Where-Object { $_.SessionID -eq $DeviceID }).Name
					} elseif ($DeviceType -eq 'rmm') {
						$Hostname = ($RMM_Devices | Where-Object { $_."Device UID" -eq $DeviceID })."Device Hostname"
					} elseif ($DeviceType -eq 'sophos') {
						$SophosDevice = ($Sophos_Devices | Where-Object { $_.id -eq $DeviceID })
						$Hostname = $SophosDevice.hostname
					}

					$Link = ''
					if ($DeviceType -eq 'sc') {
						$Link = "$($SCLogin.URL)/Host#Access/All%20Machines/$($Hostname)/$($DeviceID)"
					} elseif ($DeviceType -eq 'rmm') {
						$Link = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($DeviceID)"
					} else {
						$Link = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevice.webID)"
					}

					# See if we can try to automatically fix this issue
					$AutoFix = $false
					if (!$ReadOnly -and $DeviceType -eq 'rmm' -and $RMM_ID -and ($Device.sc_matches | Measure-Object).Count -gt 0 -and $SCLogin.Username -and $SCWebSession -and $Device.id -notin $MoveDevices.ID) {
						# Device broken in rmm but is in SC and we are using the SC api account, have the rmm org id, and this device is not in the $MoveDevices array (devices that look like they dont belong)
						$SC_Device = $SC_Devices | Where-Object { $_.SessionID -in $Device.sc_matches }
						
						# Only continue of the device was seen recently in SC (this will only work if it is active) and is using Windows
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

							if ($SCDevice.GuestOperatingSystemName -like "*Windows*" -and $SCDevice.GuestOperatingSystemName -notlike "*Windows Embedded*" -and $SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
								if (install_rmm_using_sc -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
									$AutoFix = $true
									check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
									log_change @LogParams -Company_Acronym $Company_Acronym
								}
							} elseif ($SCDevice.GuestOperatingSystemName -like "*Mac OS*" -and $SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
								if (install_rmm_using_sc_mac -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
									$AutoFix = $true
									check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
									log_change @LogParams -Company_Acronym $Company_Acronym
								}
							}
						}
					}

					if (!$ReadOnly -and $DeviceType -eq 'sc' -and $RMM_ID -and ($Device.rmm_matches | Measure-Object).Count -gt 0 -and $Device.id -notin $MoveDevices.ID) {
						# Device broken in sc but is in RMM and we are using the rmm api, and this device is not in the $MoveDevices array (devices that look like they dont belong)
						$RMM_Device = $RMM_Devices | Where-Object { $_."Device UID" -in $Device.rmm_matches }

						# Only continue of the device was seen in RMM in the last 24 hours
						foreach ($RMMDevice in $RMM_Device) {
							if ($RMMDevice.suspended -ne "True" -and ($RMMDevice.Status -eq "Online" -or $RMMDevice."Last Seen" -eq "Currently Online" -or ($RMMDevice."Last Seen" -as [DateTime]) -gt (Get-Date).AddHours(-24))) {
								if (install_sc_using_rmm -RMM_Device $RMMDevice) {
									$LogParams = @{
										ServiceTarget = "rmm"
										RMM_Device_ID = $RMMDevice."Device UID"
										ChangeType = "install_sc"
										Hostname = $RMMDevice."Device Hostname"
									}
									$AttemptCount = log_attempt_count @LogParams -LogHistory $LogHistory
									$EmailError = "ScreenConnect is broken on $($LogParams.Hostname). The Device Audit script has tried to reinstall SC via RMM $AttemptCount times now but it has not succeeded."
									$LogParams.SC_Device_ID = $Device.sc_matches
									$LogParams.Sophos_Device_ID = $Device.sophos_matches
									$LogParams.Reason = "SC connection broken"

									$AutoFix = $true
									check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
									log_change @LogParams -Company_Acronym $Company_Acronym
								}
							}
						}
					}

					$BrokenConnections += [PsCustomObject]@{
						BrokenType = $DeviceType
						Hostname = $Hostname
						LastActive = $Activity[$i].last_active
						AutoFix_Attempted = $AutoFix
						SC_Time = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].last_active } else { "NA" }
						RMM_Time = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].last_active } else { "NA" }
						Sophos_Time = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].last_active } else { "NA" }
						Link = $Link
					}
				}
			}
		}
	}

	if (($BrokenConnections | Measure-Object).count -gt 0) {
		Write-Host "Warning! Broken connections were found!" -ForegroundColor Red

		[System.Windows.MessageBox]::Show('Broken connections were found and need to be fixed. These devices showed as active in 1 system but not another.', 'Broken Connections Found')
		$BrokenConnections | Out-GridView -Title "Broken Connections Found" -PassThru
	}

	Write-Host "Broken connections check complete."
	Write-Host "======================"
}

# Find devices that are in one system but not another
if ($DOMissingConnectionSearch) {
	Write-Host "Searching for devices missing a connection..."
	$MissingConnections = @()
	$Now = Get-Date

	foreach ($Device in $MatchedDevices) {
		$SCDeviceIDs = @($Device.sc_matches)
		$RMMDeviceIDs = @($Device.rmm_matches)
		$SophosDeviceIDs = @($Device.sophos_matches)

		$ActivityComparison = compare_activity($Device)
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
					$SCDevices = $SC_Devices | Where-Object { $_.SessionID -in $SCDeviceIDs }
					$Hostnames += $SCDevices.Name
					$DeviceType = $SCDevices[0].DeviceType
				}
				if ($RMMDeviceIDs) {
					$RMMDevices = $RMM_Devices | Where-Object { $_."Device UID" -in $RMMDeviceIDs }
					$Hostnames += $RMMDevices."Device Hostname"
					if ($DeviceType) {
						$DeviceType = $RMMDevices[0]."Device Type"
					}
				}
				if ($SophosDeviceIDs) {
					$SophosDevices = $Sophos_Devices | Where-Object { $_.id -in $SophosDeviceIDs }
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
					$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceIDs[0])"
				}
				if ($SophosDeviceIDs) {
					$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevices[0].webID)"
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

						if ($SCDevice.GuestOperatingSystemName -like "*Windows*" -and $SCDevice.GuestOperatingSystemName -notlike "*Windows Embedded*" -and $SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
							if (install_rmm_using_sc -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
								$AutoFix = $true
								check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
								log_change @LogParams -Company_Acronym $Company_Acronym
							}
						} elseif ($SCDevice.GuestOperatingSystemName -like "*Mac OS*" -and $SCDevice.GuestLastSeen -gt (Get-Date).AddHours(-3)) {
							if (install_rmm_using_sc_mac -SC_ID $SCDevice.SessionID -RMM_ORG_ID $RMM_ID -SCWebSession $SCWebSession) {
								$AutoFix = $true
								check_failed_attempts @LogParams -LogHistory $LogHistory -Company_Acronym $Company_Acronym -ErrorMessage $EmailError
								log_change @LogParams -Company_Acronym $Company_Acronym
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

				$MissingConnections += [PsCustomObject]@{
					Hostnames = $Hostnames -join ', '
					DeviceType = $DeviceType
					LastActive = $NewestDate
					InSC = if ($SCDeviceIDs) { "Yes" } elseif (ignore_install -Device $Device -System 'sc') { "Ignore" } else { "No" }
					InRMM = if ($RMMDeviceIDs) { "Yes" }  elseif (ignore_install -Device $Device -System 'rmm') { "Ignore" } else { "No" }
					InSophos = if ($SophosDeviceIDs) { "Yes" } elseif (ignore_install -Device $Device -System 'sophos') { "Ignore" } else { "No" }
					AutoFix_Attempted = $AutoFix
					SC_Link = $SCLink
					RMM_Link = $RMMLink
					Sophos_Link = $SophosLink
				}
			}
		}
	}

	if (($MissingConnections | Measure-Object).count -gt 0) {
		Write-Host "Warning! Devices were found that are missing in 1 or more systems (RMM, SC, or Sophos)." -ForegroundColor Red

		[System.Windows.MessageBox]::Show('Devices were found that are missing in 1 or more systems (RMM, SC, or Sophos). Consider installing the missing tool on each device.', 'Missing Connections Found')
		$MissingConnections | Out-GridView -Title "Missing Connections Found" -PassThru
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
		$ActivityComparison = compare_activity($Device)
		$Activity = $ActivityComparison.Values | Sort-Object last_active

		if (($Activity | Measure-Object).count -gt 0) {
			$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
			$Timespan = New-TimeSpan -Start $NewestDate -End $Now
			
			if ($Timespan.Days -gt $InactiveDeleteDays -or ($Activity.type -contains "rmm" -and $Timespan.Days -gt $InactiveDeleteDaysRMM)) {
				$RMMOnly = $false
				if ($Timespan.Days -lt $InactiveDeleteDays){
					$RMMOnly = $true
				}

				$Hostnames = @()
				$SCDeviceID = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].id } else { $false }
				$RMMDeviceID = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].id } else { $false }
				$SophosDeviceID = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].id } else { $false }
				$ITG_IDs = $Device.itg_matches

				$User = $false
				$OperatingSystem = $false
				$Model = $false
				$WarrantyExpiry = ''

				if ($SCDeviceID) {
					$SCDevice = $SC_Devices | Where-Object { $_.SessionID -eq $SCDeviceID }
					$Hostnames += $SCDevice.Name
					$User = $SCDevice.GuestLoggedOnUserName
					$OperatingSystem = $SCDevice.GuestOperatingSystemName
					$Model = $SCDevice.GuestMachineModel
				}
				if ($RMMDeviceID) {
					$RMMDevice = $RMM_Devices | Where-Object { $_."Device UID" -eq $RMMDeviceID }
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
					$SophosDevice = $Sophos_Devices | Where-Object { $_.id -eq $SophosDeviceID }
					$Hostnames += $SophosDevice.hostname
					if (!$User) {
						$User = $SophosDevice.LastUser
					}
					if (!$OperatingSystem) {
						$OperatingSystem = $SophosDevice.OS
					}
				}
				$Hostnames = $HostNames | Sort-Object -Unique

				$SCLink = ''
				$RMMLink = ''
				$SophosLink = ''
				if ($SCDeviceID) {
					$SCLink = "$($SCLogin.URL)/Host#Access/All%20Machines/$($SCDevice.Name)/$($SCDeviceID)"
				}
				if ($RMMDeviceID) {
					$RMMLink = "https://$($DattoAPIKey.Region).centrastage.net/csm/search?qs=uid%3A$($RMMDeviceID)"
				}
				if ($SophosDeviceID) {
					$SophosLink = "https://cloud.sophos.com/manage/devices/computers/$($SophosDevice.webID)"
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
					if ($InactiveAutoDeleteSophos -and !$ReadOnly -and $Timespan.Days -gt $InactiveAutoDeleteSophos -and $AllowDeletion) {
						$Deleted = delete_from_sophos -Sophos_Device_ID $SophosDeviceID -TenantApiHost $TenantApiHost -SophosHeader $SophosHeader
						if ($Deleted) {
							$DeleteSophos = "Yes, auto attempted"
							log_change -Company_Acronym $Company_Acronym -ServiceTarget "sophos" -Sophos_Device_ID $SophosDevice.webID -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $Device.sc_matches -ChangeType "delete" -Hostname $SCDevice.Name -Reason "Inactive"
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
								$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq  $ID }
								log_change -Company_Acronym $Company_Acronym -ServiceTarget "itg" -RMM_Device_ID $Device.rmm_matches -SC_Device_ID $Device.sc_matches -Sophos_Device_ID $Device.sophos_matches -ChangeType "delete" -Hostname $ITG_Device.attributes.name -Reason "Inactive"
							}
						}
					}
				}


				$InactiveDevices += [PsCustomObject]@{
					Hostnames = $Hostnames -join ', '
					LastActive = $NewestDate
					User = $User
					OS = $OperatingSystem
					Model = $Model
					WarrantyExpiry = $WarrantyExpiry
					InSC = if ($SCDeviceID -and !$RMMOnly) { "Yes" } elseif ($SCDeviceID) { "Yes, but don't delete yet" } else { "No" }
					InRMM = if ($RMMDeviceID) { "Yes" } else { "No" }
					InSophos = if ($SophosDeviceID -and !$RMMOnly) { "Yes" } elseif ($SophosDeviceID) { "Yes, but don't delete yet" } else { "No" }
					DeleteSC = $DeleteSC
					DeleteRMM = $DeleteRMM
					DeleteSophos = $DeleteSophos
					ArchiveITG = $DeleteITG
					SC_Time = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].last_active } else { "NA" }
					RMM_Time = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].last_active } else { "NA" }
					Sophos_Time = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].last_active } else { "NA" }
					SC_Link = $SCLink
					RMM_Link = $RMMLink
					Sophos_Link = $SophosLink
				}
			}
		}
	}

	if (($InactiveDevices | Measure-Object).count -gt 0) {
		Write-Host "Warning! Old inactive devices were found!" -ForegroundColor Red

		[System.Windows.MessageBox]::Show('Old inactive devices were found. Consider deleting the following devices from SC, RMM, and Sophos. (Check flags, we delete RMM sooner than SC and Sophos)', 'Old Inactive Devices Found')
		$InactiveDevices | Sort-Object Hostnames,LastActive | Out-GridView -Title "Old Inactive Devices Found" -PassThru
	}

	Write-Host "Inactive devices check complete."
	Write-Host "======================"
}

# If enabled, this will output a list of any inactive workstations in ITG for archival review (for initial audit for itg device documentation)
if ($DOITGArchivalReview -and $ITGConnected) {
	$PossiblyArchive = @()
	$Now = Get-Date
	foreach ($ITG_Device in $ITG_Devices) {
		if ($ITG_Device.attributes.'configuration-type-kind' -notin @('laptop', 'workstation', 'server')) {
			continue;
		}
		if ($ITG_Device.archived -eq "True") {
			continue;
		}

		$MatchedDevice = $MatchedDevices | Where-Object { $ITG_Device.id -in $_.itg_matches } | Select-Object -First 1

		if (!$MatchedDevice) {
			$PossiblyArchive += [pscustomobject]@{
				id = $ITG_Device.id
				name = $ITG_Device.attributes.name
				url = $ITG_Device.attributes.'resource-url'
			}
			continue
		}

		$ActivityComparison = compare_activity($MatchedDevice)
		$Activity = $ActivityComparison.Values | Sort-Object last_active

		if (($Activity | Measure-Object).count -gt 0) {
			$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
			$Timespan = New-TimeSpan -Start $NewestDate -End $Now
			
			if ($Timespan.Days -gt $InactiveDeleteDays) {
				$PossiblyArchive += [pscustomobject]@{
					id = $ITG_Device.id
					name = $ITG_Device.attributes.name
					url = $ITG_Device.attributes.'resource-url'
				}
			}
		}
	}

	if ($PossiblyArchive) {
		$PossiblyArchive | Out-GridView -PassThru -Title 'Devices flagged for possible archival in IT Glue'
	} else {
		Write-Host "No devices found that might need archival in IT Glue." -ForegroundColor Green
	}
}

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
		$ActivityComparison = compare_activity($Device)
		$Activity = @($ActivityComparison.Values) | Sort-Object last_active

		if (($Activity | Measure-Object).count -gt 0) {
			$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)
			$Timespan = New-TimeSpan -Start $NewestDate -End $Now

			if ($Timespan.TotalHours -gt 6) {
				# The device has not been seen in the last 6 hours, lets just skip it
				continue;
			}

			$LastActive = $NewestDate;
			$SCDevices = $SC_Devices | Where-Object { $_.SessionID -in $Device.sc_matches }

			# If this device exists in SC, lets make sure it was also recently logged into (not just on)
			$SCLastActive = ($SCDevices | Sort-Object -Property GuestLastActivityTime | Select-Object -Last 1).GuestLastActivityTime
			if ($SCLastActive -and (New-TimeSpan -Start $SCLastActive -End $Now).TotalHours -gt 6) { 
				# online but not logged in, skip it
				continue;
			} elseif ($SCLastActive) {
				# replace $LastActive with the last active time
				$LastActive = $SCLastActive
			}

			$RMMDevices = $RMM_Devices | Where-Object { $_."Device UID" -in $Device.rmm_matches }

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
				$Domain = $RMMDevice.domain
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
				if ($Manufacturer -like "*/*") {
					$Manufacturer = ($Manufacturer -split '/')[0]
				}
				$Manufacturer = $Manufacturer.Trim()
				$Manufacturer = $Manufacturer -replace ",? ?(Inc\.?$|Corporation$|Corp\.?$|Ltd\.?$)", ""
				$Manufacturer = $Manufacturer.Trim()
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

			# Get the User ID, if not already in DB, add a new user
			$User = $ExistingUsers | Where-Object { $_.Username -like $Username }

			if (!$User) {
				# Add user
				$UserID = $([Guid]::NewGuid().ToString())
				$User = @{
					id = $UserID
					Username = $Username
					DomainOrLocal = if ($Domain) { "Domain" } else { "Local" }
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
				if (!$User.DomainOrLocal) {
					$User.DomainOrLocal = if ($Domain -and $Domain -ne "WORKGROUP") { "Domain" } else { "Local" }
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
	if (!$StatsLastUpdated -or ($StatsLastUpdated -and (Get-Date $StatsLastUpdated.LastUpdated) -lt $CheckDate)) {
		# Get all usage documents
		$Year_Month = Get-Date (Get-Date).AddMonths(-1) -Format 'yyyy-MM'
		$Usage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -Query "SELECT * FROM Usage AS u WHERE u.yearmonth = '$Year_Month'" -PartitionKey $Year_Month

		# Calculate monthly stats
		if ($Usage) {
			# Get all existing monthly stats
			$ComputerIDs = $Usage.ComputerID | Select-Object -Unique
			$Query = "SELECT * FROM ComputerUsage AS cu WHERE cu.id IN ('$($ComputerIDs -join "', '")')"
			$Existing_ComputerUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "ComputerUsage" -Query $Query -QueryEnableCrossPartition $true

			$UserIDs = $Usage.UserID | Select-Object -Unique
			$Query = "SELECT * FROM UserUsage AS uu WHERE uu.id IN ('$($UserIDs -join "', '")')"
			$Existing_UserUsage = Get-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "UserUsage" -Query $Query -QueryEnableCrossPartition $true

			# Group all usage stats from this past month by computer, user, and computer/user
			$Monthly_UsageByComputer = $Usage | Select-Object ComputerID, UserID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property ComputerID
			$Monthly_UsageByUser = $Usage | Select-Object UserID, ComputerID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property UserID
			$Monthly_UsageByComputerUser = $Usage | Select-Object ComputerID, UserID, UseDateTime, @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Group-Object -Property ComputerID, UserID
			$Monthly_OutOfDays = ($Usage | Select-Object @{Name="Day"; E={ Get-Date $_.UseDateTime -Format 'dd' }} | Select-Object -ExpandProperty Day | Sort-Object -Unique | Measure-Object).Count

			# Build the monthly stats for each computer
			$Updated_ComputerUsage = @()
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

				$New_UsageHistory.DaysActive.Total += $DaysActive
				$New_UsageHistory.DaysActive.LastMonth = $DaysActive
				$New_UsageHistory.DaysActive.LastMonthPercent = $DaysActivePercent
				if ($New_UsageHistory.DaysActive.History -is 'PSCustomObject') {
					$New_UsageHistory.DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
					$New_UsageHistory.DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
					$New_UsageHistory.DaysActive.History.$Year_Month = $DaysActive
					$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
				} else {
					$New_UsageHistory.DaysActive.History[$Year_Month] = $DaysActive
					$New_UsageHistory.DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
				}
				$New_UsageHistory.LastActive = ($MonthsUsage.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
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
						$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.Total += $DaysActive
						$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.LastMonth = $DaysActive
						$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.LastMonthPercent = $DaysActivePercent
						if ($New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History -is 'PSCustomObject') {
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History.$Year_Month = $DaysActive
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
						} else {
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.History[$Year_Month] = $DaysActive
							$New_UsageHistory.UsersUsedBy[$ExistingIndex].DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
						}
						$New_UsageHistory.UsersUsedBy[$ExistingIndex].LastActive = ($MonthsUsageByUser.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
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
			$Updated_UserUsage = @()
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

				$New_UsageHistory.DaysActive.Total += $DaysActive
				$New_UsageHistory.DaysActive.LastMonth = $DaysActive
				$New_UsageHistory.DaysActive.LastMonthPercent = $DaysActivePercent
				if ($New_UsageHistory.DaysActive.History -is 'PSCustomObject') {
					$New_UsageHistory.DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
					$New_UsageHistory.DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
					$New_UsageHistory.DaysActive.History.$Year_Month = $DaysActive
					$New_UsageHistory.DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
				} else {
					$New_UsageHistory.DaysActive.History[$Year_Month] = $DaysActive
					$New_UsageHistory.DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
				}
				$New_UsageHistory.LastActive = ($MonthsUsage.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
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
						$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.Total += $DaysActive
						$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.LastMonth = $DaysActive
						$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.LastMonthPercent = $DaysActivePercent
						if ($New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History -is 'PSCustomObject') {
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent | Add-Member -NotePropertyName $Year_Month -NotePropertyValue 0
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History.$Year_Month = $DaysActive
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent.$Year_Month = $DaysActivePercent
						} else {
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.History[$Year_Month] = $DaysActive
							$New_UsageHistory.ComputersUsed[$ExistingIndex].DaysActive.HistoryPercent[$Year_Month] = $DaysActivePercent
						}
						$New_UsageHistory.ComputersUsed[$ExistingIndex].LastActive = ($MonthsUsageByComputer.Group | Sort-Object { $_.UseDateTime -as [DateTime] } -Descending | Select-Object -First 1).UseDateTime
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
		}
	}

	Write-Host "Usage Stats Saved!"
	Write-Host "===================="
}

# Get a count and full list of devices that have been used in the last $InactiveBillingDays for billing
if ($DOBillingExport) {
	Write-Host "Building a device list for billing..."
	$BillingDevices = @()
	$AllDevices = @()
	$AssetReport = @()
	$Now = Get-Date

	foreach ($Device in $MatchedDevices) {
		$ActivityComparison = compare_activity($Device)
		$Activity = @($ActivityComparison.Values) | Sort-Object last_active

		if (($Activity | Measure-Object).count -gt 0) {
			$NewestDate = [DateTime]($Activity.last_active | Sort-Object | Select-Object -Last 1)

			$Timespan = New-TimeSpan -Start $NewestDate -End $Now
			
			$SCDeviceID = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].id } else { $false }
			$RMMDeviceID = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].id } else { $false }
			$SophosDeviceID = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].id } else { $false }
			$ITGDeviceID = if (($Device.itg_matches | Measure-Object).Count -gt 0) { $Device.itg_matches[0] } else { $false }
			$AutotaskDeviceID = if (($Device.autotask_matches | Measure-Object).Count -gt 0) { $Device.autotask_matches[0] } else { $false }

			$Hostname = $false
			$SerialNumber = $false
			$Location = $false
			$AssignedUser = $false
			$DeviceType = $false
			$LastUser = $false
			$OperatingSystem = $false
			$Manufacturer = $false
			$Model = $false
			$WarrantyStart = $false
			$WarrantyExpiry = $false
			$ReplacementYear = $false

			if ($RMMDeviceID) {
				$RMMDevice = $RMM_Devices | Where-Object { $_."Device UID" -eq $RMMDeviceID }
				$Hostname = $RMMDevice."Device Hostname"
				$SerialNumber = $RMMDevice."Serial Number"
				$LastUser = ($RMMDevice."Last User" -split '\\')[1]
				$OperatingSystem = $RMMDevice."Operating System"
				$Manufacturer = $RMMDevice."Manufacturer"
				$Model = $RMMDevice."Device Model"
				$WarrantyExpiry = $RMMDevice."Warranty Expiry"
				$DeviceType = $RMMDevice."Device Type"

				if ($LastUser -and $RMMDevice."Last User" -like "$($Hostname)\*") {
					$LastUser = "$($LastUser) (Local)"
				}
			}
			if ($AutotaskDeviceID) {
				$AutotaskDevice = $Autotask_Devices | Where-Object { $_.id -eq $AutotaskDeviceID }
				if ($AutotaskDevice) {
					$AutotaskLocation = $Autotask_Locations | Where-Object { $_.id -eq $AutotaskDevice.companyLocationID }
					$AutotaskContact = $Autotask_Contacts | Where-Object { $_.id -eq $AutotaskDevice.contactID }

					if ($AutotaskLocation) {
						$Location = $AutotaskLocation.name
					}
					if ($AutotaskContact) {
						$AssignedUser = $AutotaskContact.firstName + " " + $AutotaskContact.lastName
					}
					if (!$Hostname) {
						$Hostname = $AutotaskDevice.rmmDeviceAuditHostname
					}
					if (!$Hostname) {
						$Hostname = $AutotaskDevice.referenceTitle
					}
					if (!$SerialNumber) {
						$SerialNumber = $AutotaskDevice.serialNumber
					}
					if (!$LastUser -and $AutotaskDevice.rmmDeviceAuditLastUser) {
						$LastUser = ($AutotaskDevice.rmmDeviceAuditLastUser -split '\\')[1]
						if ($LastUser -and $AutotaskDevice.rmmDeviceAuditLastUser -like "$($Hostname)\*") {
							$LastUser = "$($LastUser) (Local)"
						}
					}
					if (!$OperatingSystem) {
						$OperatingSystem = $AutotaskDevice.rmmDeviceAuditOperatingSystem
					}
					if ($AutotaskDevice.warrantyExpirationDate -and [string]$AutotaskDevice.warrantyExpirationDate -as [DateTime]) {
						$WarrantyExpiry = $AutotaskDevice.warrantyExpirationDate
					}
					$WarrantyStart = ($AutotaskDevice.userDefinedFields | Where-Object { $_.name -eq "Warranty Start Date" }).value
				}
			}
			if ($ITGDeviceID) {
				$ITGDevice = $ITG_Devices | Where-Object { $_.id -eq $ITGDeviceID }
				if (!$Location) {
					$Location = $ITGDevice.attributes."location-name"
				}
				if (!$AssignedUser) {
					$AssignedUser = $ITGDevice.attributes."contact-name"
				}
				if (!$SerialNumber) {
					$SerialNumber = $ITGDevice.attributes.'serial-number'
				}
				if (!$DeviceType) {
					$DeviceType = $ITGDevice.attributes."configuration-type-name"
				}
				if (!$WarrantyExpiry) {
					$WarrantyExpiry = $ITGDevice.attributes.'warranty-expires-at'
				}
				if (!$WarrantyStart) {
					$WarrantyStart = $ITGDevice.attributes.'purchased-at'
				}
			}
			if ($SCDeviceID) {
				$SCDevice = $SC_Devices | Where-Object { $_.SessionID -eq $SCDeviceID }
				if (!$Hostname) {
					$Hostname = $SCDevice.Name
				}
				if (!$SerialNumber) {
					$SerialNumber = $SCDevice.GuestMachineSerialNumber
				}
				if (!$DeviceType) {
					$DeviceType = $SCDevice.DeviceType
				}
				if (!$LastUser) {
					$LastUser = $SCDevice.GuestLoggedOnUserName
				}
				if ($SCDevice.GuestOperatingSystemName) {
					$OperatingSystem = $SCDevice.GuestOperatingSystemName
				}
				if (!$Manufacturer) {
					$Manufacturer = $SCDevice.GuestMachineManufacturerName
				}
				if (!$Model) {
					$Model = $SCDevice.GuestMachineModel
				}
			}
			if ($SophosDeviceID) {
				$SophosDevice = $Sophos_Devices | Where-Object { $_.id -eq $SophosDeviceID }
				if (!$Hostname) {
					$Hostname = $SophosDevice.hostname
				}
				if (!$LastUser) {
					$LastUser = $SophosDevice.LastUser
				}
				if (!$DeviceType) {
					$DeviceType = $SophosDevice.Type
				}
				if (!$OperatingSystem) {
					$OperatingSystem = $SophosDevice.OS
				}
			}

			if ($WarrantyStart -and [string]$WarrantyStart -as [DateTime]) {
				$ReplacementYear = (([DateTime]$WarrantyStart).AddYears(5)).Year
			} elseif ($WarrantyExpiry -and [string]$WarrantyExpiry -as [DateTime]) {
				$ReplacementYear = (([DateTime]$WarrantyExpiry).AddYears(2)).Year
			}

			# cleanup data to be more readable
			if ($Manufacturer) {
				if ($Manufacturer -like "*/*") {
					$Manufacturer = ($Manufacturer -split '/')[0]
				}
				$Manufacturer = $Manufacturer.Trim()
				$ManufacturerCleanup | Foreach-Object { 
					if ($Manufacturer -like $_.name -or $Manufacturer -match $_.name) {
						if ($_.caseSensitive) {
							$Manufacturer = $Manufacturer -creplace $_.name, $_.replacement
						} else {
							$Manufacturer = $Manufacturer -replace $_.name, $_.replacement
						}
					}
				}
				$Manufacturer = $Manufacturer.Trim()
			}

			if (!$Manufacturer) {
				$Manufacturer = "Custom Build"
			}
			
			if ($Model) {
				$Model = $Model -replace "System Product Name", "Custom Build"
				$Model = $Model -replace "To be filled by O.E.M.", "Custom Build"
				$Model = $Model -replace $Manufacturer, ""
				$Model = $Model.Trim();
			}
			if (!$Model) {
				$Model = "Custom Build"
			}

			if ($SerialNumber) {
				if ($SerialNumber -in $IgnoreSerials) {
					$SerialNumber = ''
				}
			}

			if ($OperatingSystem) {
				if ($OperatingSystem -like "Microsoft*" -or $OperatingSystem -like "Windows*") {
					$OperatingSystem = $OperatingSystem -replace " ?(((\d+)\.*)+)$", ""
					$OperatingSystem = $OperatingSystem -replace " Service Pack ?\d?\d?$", ""
					$OperatingSystem = $OperatingSystem -replace "Microsoft ", ""
					$OperatingSystem = $OperatingSystem -replace "Professional", "Pro"
				} elseif ($OperatingSystem -like "VMware*") {
					$OperatingSystem = $OperatingSystem -replace " ?(build\d*) (((\d+)\.*)+)$", ""
				} elseif ($OperatingSystem -like "Linux*") {
					if ($OperatingSystem -match "(\w+\s\w+)[^0-9]*(\d\d?).*") {
						$OperatingSystem = $Matches[1] + " " + $Matches[2]
					}
				}
			}

			if ($WarrantyExpiry) {
				$WarrantyExpiry = $WarrantyExpiry -replace " UTC$", ""
				$WarrantyExpiry = ([DateTime]$WarrantyExpiry).ToString("yyyy-MM-dd")
			}
			if ($WarrantyStart) {
				$WarrantyStart = $WarrantyStart -replace " UTC$", ""
				$WarrantyStart = ([DateTime]$WarrantyStart).ToString("yyyy-MM-dd")
			}

			if (!$DeviceType) {
				if ($OperatingSystem -and $OperatingSystem -like "*Server*") {
					$DeviceType = "Server"
				} else {
					$DeviceType = "Workstation"
				}
			}

			if (!$Hostname) { $Hostname = "" }
			if (!$SerialNumber) { $SerialNumber = "" }
			if (!$Location) { $Location = "" }
			if (!$AssignedUser) { $AssignedUser = "" }
			if (!$DeviceType) { $DeviceType = "" }
			if (!$LastUser) { $LastUser = "" }
			if (!$OperatingSystem) { $OperatingSystem = "" }
			if (!$Manufacturer) { $Manufacturer = "" }
			if (!$Model) { $Model = "" }
			if (!$WarrantyStart) { $WarrantyStart = "" }
			if (!$WarrantyExpiry) { $WarrantyExpiry = "" }
			if (!$ReplacementYear) { $ReplacementYear = "" }

			# Count as billed if not inactive, ignore devices only in sophos and not seen in the past week as they were likely decommissioned, and
			# ignore devices that appear to be under the wrong company
			$Billed = $true
			$DoAssetReport = $true
			$BilledStr = "Yes"
			if ($Timespan.Days -ge $InactiveBillingDays) {
				$Billed = $false
				$BilledStr = "No (Inactive)"
			} 
			if (!$RMMDeviceID -and !$SCDeviceID -and $Timespan.Days -gt 7) {
				$Billed = $false
				$DoAssetReport = $false
				$BilledStr = "No (Decommissioned)"
			}
			if ($Device.id -in $MoveDevices.ID) {
				$Billed = $false
				$DoAssetReport = $false
				$BilledStr = "No (Wrong Company?)"
			}
			
			if ($Billed) {

				$BillingDevices += [PsCustomObject]@{
					Hostname = $Hostname
					DeviceType = $DeviceType
					LastUser = $LastUser
					SerialNumber = $SerialNumber
					Manufacturer = $Manufacturer
					Model = $Model
					OS = $OperatingSystem
					LastActive = $NewestDate
					WarrantyExpiry = $WarrantyExpiry
				}
			}

			$AllDevices += [PsCustomObject]@{
				Hostname = $Hostname
				DeviceType = $DeviceType
				LastUser = $LastUser
				SerialNumber = $SerialNumber
				Manufacturer = $Manufacturer
				Model = $Model
				OS = $OperatingSystem
				LastActive = $NewestDate
				WarrantyExpiry = $WarrantyExpiry
				Billed = $BilledStr
				InSC = if ($SCDeviceID) { "Yes" } else { "No" }
				InRMM = if ($RMMDeviceID) { "Yes" } else { "No" }
				InSophos = if ($SophosDeviceID) { "Yes" } else { "No" }
				SC_Time = if ($ActivityComparison.sc) { $ActivityComparison.sc[0].last_active } else { "NA" }
				RMM_Time = if ($ActivityComparison.rmm) { $ActivityComparison.rmm[0].last_active } else { "NA" }
				Sophos_Time = if ($ActivityComparison.sophos) { $ActivityComparison.sophos[0].last_active } else { "NA" }
			}

			if ($DeviceType -eq "Server" -or $Model -like "*Virtual *" -or !$DoAssetReport) {
				continue
			}		

			$AssetReport += [PsCustomObject]@{
				Device = $Hostname
				Location = $Location
				"Assigned User" = $AssignedUser
				"Last Login" = $LastUser
				"Active?" = if ($BilledStr -eq "Yes") { "X" } else { "" }
				Make = $Manufacturer
				Model = $Model
				Serial = $SerialNumber
				"Operating System" = $OperatingSystem
				Purchased = $WarrantyStart
				"Warranty Expiry" = $WarrantyExpiry
				"Replacement Year" = $ReplacementYear
				"Suggested Replacement" = ""
				"Replacement Budget" = ""
				Notes = if ($BilledStr -ne "Yes") { "Last Seen: " + $NewestDate.ToString("yyyy-MM-dd") } else { "" }
			}
		}
	}

	if (($BillingDevices | Measure-Object).count -gt 0) {
		# Get device type counts
		$DeviceCounts = @()
		$DeviceCounts += [PSCustomObject]@{
			Type = "Servers"
			Count = @($BillingDevices | Where-Object { $_.DeviceType -like "Server" }).count
		}
		$DeviceCounts += [PSCustomObject]@{
			Type = "Workstations"
			Count = @($BillingDevices | Where-Object { $_.DeviceType -notlike "Server" }).count
		}

		$FullDeviceCounts = @()
		$FullDeviceCounts += [PSCustomObject]@{
			Type = "Servers"
			BilledCount = @($AllDevices | Where-Object { $_.DeviceType -like "Server" -and $_.Billed -like "Yes*" }).count
			UnBilledCount = @($AllDevices | Where-Object { $_.DeviceType -like "Server" -and $_.Billed -like "No*" }).count
		}
		$FullDeviceCounts += [PSCustomObject]@{
			Type = "Workstations"
			BilledCount = @($AllDevices | Where-Object { $_.DeviceType -notlike "Server" -and $_.Billed -like "Yes*" }).count
			UnBilledCount = @($AllDevices | Where-Object { $_.DeviceType -notlike "Server" -and $_.Billed -like "No*" }).count
		}

		# Get server breakdown counts
		$ServerCounts = @()
		$AllServers = @($AllDevices | Where-Object { $_.DeviceType -like "Server" -and $_.Billed -like "Yes*" })
		$ServerCounts += [PSCustomObject]@{
			Type = "Physical"
			Count = @($AllServers | Where-Object { $_.Hostname -notlike "*BDR*" -and $_.Model -and $_.Model -notlike "*Virtual*" }).count
		}
		$ServerCounts += [PSCustomObject]@{
			Type = "Virtual"
			Count = @($AllServers | Where-Object { $_.Hostname -notlike "*BDR*" -and ($_.Model -like "*Virtual*" -or !$_.Model) }).count
		}
		$ServerCounts += [PSCustomObject]@{
			Type = "BDR (Backup)"
			Count = @($AllServers | Where-Object { $_.Hostname -like "*BDR*" }).count
		}

		# $BillingDevices | Sort-Object -Property DeviceType, Hostname | Out-GridView -Title "Device List" -PassThru

		# Create an excel document (for customers)
		$MonthName = (Get-Culture).DateTimeFormat.GetMonthName([int](Get-Date -Format MM))
		$Year = Get-Date -Format yyyy
		$FileName = "$($Company_Acronym)--Device_List--$($MonthName)_$Year.xlsx"
		$Path = $PSScriptRoot + "\$FileName"
		Remove-Item $Path -ErrorAction SilentlyContinue

		$BillingDevices | Sort-Object -Property DeviceType, Hostname | 
			Export-Excel $Path -WorksheetName "Device List" -AutoSize -AutoFilter -NoNumberConversion * -TableName "DeviceList" -Title "Device List" -TitleBold -TitleSize 18
		$excel = $DeviceCounts | Export-Excel $Path -WorksheetName "Device Counts" -AutoSize -PassThru -Title "Device Count" -TitleBold -TitleSize 18
		$ws_counts = $excel.Workbook.Worksheets['Device Counts']
		Add-ExcelTable -PassThru -Range $ws_counts.Cells["A2:B4"] -TableName DeviceCounts -TableStyle "Light21" -ShowFilter:$false -ShowTotal -ShowFirstColumn -TotalSettings @{"Count" = "Sum"} | Out-Null
		$xlParams = @{WorkSheet=$ws_counts; Bold=$true; FontSize=18; Merge=$true}
		Set-ExcelRange -Range "A7:B7" -Value "Server Breakdown" @xlParams
		$excel = $ServerCounts | Export-Excel -PassThru -ExcelPackage $excel -WorksheetName $ws_counts -AutoSize -StartRow 8 -TableName ServerBreakdown -TableStyle "Light21"
		Add-ExcelTable -PassThru -Range $ws_counts.Cells["A8:B10"] -TableName ServerBreakdown -TableStyle "Light21" -ShowFilter:$false -ShowTotal -ShowFirstColumn -TotalSettings @{"Count" = "Sum"} | Out-Null

		Close-ExcelPackage $excel

		# Move file (if applicable)
		if ($MoveCustomerList.Location -and (Test-Path -Path $MoveCustomerList.Location)) {
			if ($MoveCustomerList.Copy) {
				Copy-Item -Path $Path -Destination $MoveCustomerList.Location -Force
			} else {
				Move-Item -Path $Path -Destination $MoveCustomerList.Location -Force
			}	
		}

		# Create a second excel document (for techs with extra info)
		$FileName = "$($Company_Acronym)--Device_List--$($MonthName)_$Year--ForTechs.xlsx"
		$Path = $PSScriptRoot + "\$FileName"
		Remove-Item $Path -ErrorAction SilentlyContinue

		$AllDevices | Sort-Object -Property DeviceType, Hostname | Export-Excel $Path -WorksheetName "Device List" -AutoSize -AutoFilter -NoNumberConversion * -TableName "DeviceList" -Title "Full Device List" -TitleBold -TitleSize 18
		$excel = $FullDeviceCounts | Export-Excel $Path -WorksheetName "Device Counts" -AutoSize -PassThru -Title "Full Device Count" -TitleBold -TitleSize 18
		$ws_counts = $excel.Workbook.Worksheets['Device Counts']
		Add-ExcelTable -PassThru -Range $ws_counts.Cells["A2:C4"] -TableName DeviceCounts -TableStyle "Light21" -ShowFilter:$false -ShowTotal -ShowFirstColumn -TotalSettings @{"BilledCount" = "Sum"; "UnBilledCount" = "Sum"} | Out-Null
		$xlParams = @{WorkSheet=$ws_counts; Bold=$true; FontSize=18; Merge=$true}
		Set-ExcelRange -Range "A7:B7" -Value "Server Breakdown" @xlParams
		$excel = $ServerCounts | Export-Excel -PassThru -ExcelPackage $excel -WorksheetName $ws_counts -AutoSize -StartRow 8 -TableName ServerBreakdown -TableStyle "Light21"
		Add-ExcelTable -PassThru -Range $ws_counts.Cells["A8:B10"] -TableName ServerBreakdown -TableStyle "Light21" -ShowFilter:$false -ShowTotal -ShowFirstColumn -TotalSettings @{"Count" = "Sum"} | Out-Null

		Close-ExcelPackage $excel -Show

		# Create/update a third excel document, the asset report
		$FileName = "$($Company_Acronym)--Asset_Report.xlsx"
		$Path = $PSScriptRoot + "\$FileName"
		Remove-Item $Path -ErrorAction SilentlyContinue

		$excel = $AssetReport | Sort-Object -Property Device | Export-Excel $Path -WorksheetName "Asset Report" -AutoSize -AutoFilter -NoNumberConversion * -TableName "AssetReport" -PassThru
		$rowCount = ($AssetReport | Measure-Object).Count
		$curYear = get-date -Format yyyy
		$ws_report = $excel.Workbook.Worksheets['Asset Report']
		$ws_report.Cells["K2:K$($rowCount+1)"].Style.HorizontalAlignment="Center"
		$OrangeColor = [System.Drawing.Color]::FromArgb(255,192,0)
		$GreenColor = [System.Drawing.Color]::FromArgb(146,208,80)
		Add-ConditionalFormatting -Worksheet $ws_report -Address "K2:K$($rowCount+1)" -RuleType ContainsBlanks -StopIfTrue
		Add-ConditionalFormatting -Worksheet $ws_report -Address "K2:K$($rowCount+1)" -RuleType LessThanOrEqual -ForegroundColor black -BackgroundColor red -ConditionValue ($curYear - 3)
		Add-ConditionalFormatting -Worksheet $ws_report -Address "K2:K$($rowCount+1)" -RuleType Equal -ForegroundColor black -BackgroundColor $OrangeColor -ConditionValue ($curYear - 2)
		Add-ConditionalFormatting -Worksheet $ws_report -Address "K2:K$($rowCount+1)" -RuleType Equal -ForegroundColor black -BackgroundColor yellow -ConditionValue ($curYear - 1)
		Add-ConditionalFormatting -Worksheet $ws_report -Address "K2:K$($rowCount+1)" -RuleType GreaterThanOrEqual -ForegroundColor black -BackgroundColor $GreenColor -ConditionValue ($curYear)
		
		Close-ExcelPackage $excel

		# Move file (if applicable) (force it to always be a copy so that we can open it)
		if ($MoveTechList.Location -and (Test-Path -Path $MoveTechList.Location)) {
			$FileName = "$($Company_Acronym)--Device_List--$($MonthName)_$Year--ForTechs.xlsx"
			$Path = $PSScriptRoot + "\$FileName"
			Copy-Item -Path $Path -Destination $MoveTechList.Location -Force
		}
		if ($MoveAssetReport.Location -and (Test-Path -Path $MoveAssetReport.Location)) {
			$FileName = "$($Company_Acronym)--Asset_Report.xlsx"
			$Path = $PSScriptRoot + "\$FileName"
			Copy-Item -Path $Path -Destination $MoveAssetReport.Location -Force
		}

		Write-Host "Device list exported. See: $($FileName)" -ForegroundColor Green
	} else {
		Write-Host "Something went wrong! No devices were found for the billing list." -ForegroundColor Red
	}

	Write-Host "Device list built."
	Write-Host "======================"
}

# Update device locations in Autotask/IT Glue
if ($DOUpdateDeviceLocations -and $ITGConnected -and $AutotaskConnected -and $ITG_ID -and $Autotask_ID) {
	Write-Host "Updating device locations..."
	$WANs = Get-ITGlueFlexibleAssets -page_size 1000 -filter_flexible_asset_type_id $WANFilterID.id -filter_organization_id $ITG_ID
	$LANs = Get-ITGlueFlexibleAssets -page_size 1000 -filter_flexible_asset_type_id $LANFilterID.id -filter_organization_id $ITG_ID
	$ITGLocations = Get-ITGlueLocations -org_id $ITG_ID
	if ($OverviewFilterID) {
		$CustomOverviews = Get-ITGlueFlexibleAssets -filter_flexible_asset_type_id $OverviewFilterID.id -filter_organization_id $ITG_ID
		$WANCustomOverviews = $CustomOverviews.data | Where-Object { $_.attributes.name -like "WAN: *" }
		$LANCustomOverviews = $CustomOverviews.data | Where-Object { $_.attributes.name -like "LAN: *" }
	}
	$AutotaskLocations = Get-AutotaskAPIResource -Resource CompanyLocations -SimpleSearch "companyID eq $Autotask_ID"
	$AutotaskLocations = $AutotaskLocations | Where-Object { $_.isActive -eq "True" }
	$IPRegex = "\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(-(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)?)?(\/[1-3][0-9])?\b"

	if ($LANs -and $LANs.data) {
		$LANs = $LANs.data
	}

	if ($WANs -and $WANs.data -and ($WANs.data | Measure-Object).Count -gt 0 -and $ITG_Devices -and $ITGLocations -and $AutotaskLocations) {
		$WANs = $WANs.data
		$ITGLocations = $ITGLocations.data

		$LocationIPs = @()
		$LANIPs = @{}

		foreach ($Location in $ITGLocations) {
			$LocationLANs = $LANs | Where-Object { $_.attributes.traits.'location-s'.values.id -contains $Location.id }
			$LocationWANs = @()
			$LocationWANs += $WANs | Where-Object { $_.attributes.traits.'location-s'.values.id -contains $Location.id }
			if ($LocationLANs) {
				$LocationWANs += $WANs | Where-Object { $_.id -in $LocationLANs.attributes.traits.'internet-wan'.values.id }	
			}
			$LocationWANs = $LocationWANs | Sort-Object -Unique

			if (!$LocationWANs) {
				continue
			}

			$IPs_Parsed = @()
			foreach ($WAN in $LocationWANs) {
				$IPAddressInfo = $WAN.attributes.traits.'ip-address-es'
				$IPHTML = ""
	
				# Parse the html on the WAN page
				if ($IPAddressInfo -like "*<table>*") {
					$HTML = New-Object -Com "HTMLFile"
					$HTML.IHTMLDocument2_write($IPAddressInfo)
	
					$TableData = $HTML.all | Where-Object { $_.tagname -eq 'td' }
					$TableHeaders = $TableData | Where-Object { $_.innerHtml -like "*<strong>*</strong>*" }
					$TableData = $TableData | Where-Object { $_.innerHtml -notlike "*<strong>*</strong>*" }
	
					$ColCount = $TableHeaders.Count
					for ($i = 0; $i -le $TableData.count; $i++) {
						$Column = $i % $ColCount
						$Header = $TableHeaders[$Column].innerHTML
						if ($Header -like "*DNS*" -or $Header -like "*Subnet*") {
							continue
						}
						$IPHTML += "`n$($TableData[$i].innerHTML)"
					}
				} elseif ($IPAddressInfo -like "*:*") {
					$IPHTML = $IPAddressInfo -replace "Subnet(<.*>)?:?(<.*>)? ?$IPRegex", '' -replace "DNS(<.*>)?:?(<.*>)? ?$IPRegex", ''
				} else {
					$IPHTML = $IPAddressInfo
				}
	
				# Find all IP's in the html, parse ranges and masks if needed, then map them to their locations
				$Matches = [RegEx]::Matches($IPHTML, $IPRegex)

				if ($Matches -and $Matches.value) {
					$IPs = @($Matches.Value)
					foreach ($IP in $IPs) {
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
								$IPs_Parsed += "$($Octets[0]).$($Octets[1]).$($Octets[2]).$($EndingOctet)"
							}
						} elseif ($IP -like "*/*") {
							$IPRange = Get-Subnet $IP
							$IPs_Parsed += $IPRange.IPAddress.IPAddressToString
							$IPRange.HostAddresses | ForEach-Object {
								$IPs_Parsed += $_
							}
							$IPs_Parsed += $IPRange.BroadcastAddress.IPAddressToString
						} else {
							$IPs_Parsed += $IP
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
			
			$AutotaskLocation = $AutotaskLocations | Where-Object { $_.name -like $Location.attributes.name }
			if (!$AutotaskLocation) {
				$AutotaskLocation = $AutotaskLocations | Where-Object {
					$_.address1 -like $Location.attributes.'address-1' -and
					$_.address2 -like $Location.attributes.'address-2' -and
					$_.city -like $Location.attributes.city -and
					$_.postalCode -like $Location.attributes.'postal-code' -and
					$_.state -like $Location.attributes.'region-name' -and
					($_.phone -replace "[^0-9]") -like $Location.attributes.phone
				}
			}

			$LocationIPs += [PSCustomObject]@{
				ExternalIPs = $IPs_Parsed
				InternalIPs = $InternalIPs
				ITGLocation = $Location.id
				AutotaskLocation = $AutotaskLocation.id
				WANs = @($LocationWANs.id)
				LANs = @($ValidLANs)
			}
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

				if (!$MatchedDevice.itg_matches -or !$MatchedDevice.autotask_matches) {
					continue
				}

				$RMMDevice = $RMM_Devices | Where-Object { $_.'Device UID' -in $MatchedDevice.rmm_matches }
				if ($RMMDevice) {
					$ExternalIP = @($RMMDevice.extIpAddress)
					$InternalIP = @($RMMDevice.intIpAddress)
				} else {
					$AutotaskDevice = $Autotask_Devices | Where-Object { $_.id -in $MatchedDevice.autotask_matches }
					$ExternalIP = @($AutotaskDevice.rmmDeviceAuditExternalIPAddress)
					$InternalIP = @($AutotaskDevice.rmmDeviceAuditIPAddress)
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

				$PossibleLocations = $PossibleLocations | Where-Object { $_.AutotaskLocation }

				if (!$PossibleLocations) {
					continue
				}
				
				# Populate WAN and LAN device lists for custom overviews
				foreach ($ITG_DeviceID in $MatchedDevice.itg_matches) {
					$ITGMatch = $ITG_Devices | Where-Object { $_.id -eq $ITG_DeviceID }

					# If currently set location is in $PossibleLocations, use existing location
					if ($ITGMatch.attributes.'location-id' -and $ITGMatch.attributes.'location-id' -in $PossibleLocations.Location) {
						$ExistingLocation = $PossibleLocations | Where-Object { $ITGMatch.'location-id' -in $_.Location } | Select-Object -First 1
						if ($ExistingLocation.WANs) {
							foreach ($WAN_ID in $ExistingLocation.WANs) {
								$WANDevices[$WAN_ID] += $MatchedDevice.id
							}
						}
					} else {
						# Otherwise use the newly chosen location
						$NewLocation = $PossibleLocations | Select-Object -First 1
						if ($NewLocation.WANs) {
							foreach ($WAN_ID in $NewLocation.WANs) {
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

				foreach ($Autotask_DeviceID in $MatchedDevice.autotask_matches) {
					$AutotaskMatch = $Autotask_Devices | Where-Object { $_.id -eq $Autotask_DeviceID }
					# If currently set location is in $PossibleLocations, dont update
					if ($AutotaskMatch.companyLocationID -in $PossibleLocations.AutotaskLocation) {
						continue
					}

					# Update location
					Write-Progress -Activity "Updating Device Locations" -PercentComplete $PercentComplete -Status ("Working - " + $PercentComplete + "% (Updating: $Hostname)")
					$NewLocation = $PossibleLocations | Select-Object -First 1 # if multiple, just use the first

					$ConfigurationUpdate = 
					[PSCustomObject]@{
						id = $Autotask_DeviceID
						companyLocationID = $NewLocation.AutotaskLocation
					}

					Set-AutotaskAPIResource -Resource ConfigurationItems -ID $Autotask_DeviceID -body $ConfigurationUpdate | Out-Null
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
		if ($WANDevices) {
			foreach ($WAN_ID in $WANDevices.keys) {
				$WAN = $WANs | Where-Object { $_.id -eq $WAN_ID }
				$Title = "WAN: Seen Devices - $($WAN.attributes.traits.label)"
				$ExistingOverview = $WANCustomOverviews | Where-Object { $_.attributes.traits.label -like $Title -or $_.attributes.traits.overview -like "*WAN ID: '$WAN_ID'*" } | Select-Object -First 1
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
					$MatchedDevice = $MatchedDevices | Where-Object { $_.id -eq $MatchID }
					$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
					$ITG_DeviceID = $null
					if (($MatchedDevice.itg_matches | Measure-Object).Count -gt 0) {
						$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq ($MatchedDevice.itg_matches | Select-Object -First 1) }
						$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"
						$ITG_DeviceID = $ITG_Device.id
					}

					if ($PreviousWANHistory -and ($Hostname -in $DevicePreviousHistory -or ($ITG_DeviceID -and $ITG_DeviceID -in $DevicePreviousHistory.DeviceID))) {
						# Remove devices from previously seen if in current list
						$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceName -notlike $Hostname }
						if ($ITG_DeviceID) {
							$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceID -ne $ITG_DeviceID }
						}
					}

					if ($Hostname -in $DeviceHistory.DeviceName) {
						continue
					}

					$ActivityComparison = compare_activity($MatchedDevice)
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
						$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq ($MatchedDevice.itg_matches | Select-Object -First 1) }
						$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"

						if ($Hostname -in $DeviceHistory.DeviceName -or ($Device.DeviceID -and $Device.DeviceID -in $DeviceHistory.DeviceID)) {
							continue
						}

						$Row = [PSCustomObject]@{
							'DeviceName' = $Hostname
							'Device' = $HostnameAndURL
							'Last Seen' = $Device.LastSeen.ToLocalTime()
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
					$New_WANOverview = New-ITGlueFlexibleAssets -organization_id $ITG_ID -data $FlexAssetBody
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
					$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq $Device.DeviceID }

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

		if ($LANDevices) {
			foreach ($LAN_ID in $LANDevices.keys) {
				$LAN = $LANs | Where-Object { $_.id -eq $LAN_ID }
				$Title = "LAN: Seen Devices - $($LAN.attributes.traits.name)"
				$ExistingOverview = $LANCustomOverviews | Where-Object { $_.attributes.traits.name -like $Title -or $_.attributes.traits.overview -like "*LAN ID: '$LAN_ID'*" } | Select-Object -First 1
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
					$MatchedDevice = $MatchedDevices | Where-Object { $_.id -eq $MatchID }
					$Hostname = @($MatchedDevice.sc_hostname + $MatchedDevice.rmm_hostname + $MatchedDevice.sophos_hostname + $MatchedDevice.itg_hostname + $MatchedDevice.autotask_hostname) | Select-Object -First 1
					$ITG_DeviceID = $null
					if (($MatchedDevice.itg_matches | Measure-Object).Count -gt 0) {
						$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq ($MatchedDevice.itg_matches | Select-Object -First 1) }
						$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"
						$ITG_DeviceID = $ITG_Device.id
					}

					if ($PreviousLANHistory -and ($Hostname -in $DevicePreviousHistory -or ($ITG_DeviceID -and $ITG_DeviceID -in $DevicePreviousHistory.DeviceID))) {
						# Remove devices from previously seen if in current list
						$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceName -notlike $Hostname }
						if ($ITG_DeviceID) {
							$DevicePreviousHistory = $DevicePreviousHistory | Where-Object { $_.DeviceID -ne $ITG_DeviceID }
						}
					}

					if ($Hostname -in $DeviceHistory.DeviceName) {
						continue
					}

					$ActivityComparison = compare_activity($MatchedDevice)
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
						$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq ($MatchedDevice.itg_matches | Select-Object -First 1) }
						$HostnameAndURL = "<a href='$($ITG_Device.attributes.'resource-url')'>$Hostname</a>"

						if ($Hostname -in $DeviceHistory.DeviceName -or ($Device.DeviceID -and $Device.DeviceID -in $DeviceHistory.DeviceID)) {
							continue
						}

						$Row = [PSCustomObject]@{
							'DeviceName' = $Hostname
							'Device' = $HostnameAndURL
							'Last Seen' = $Device.LastSeen.ToLocalTime()
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
					$New_LANOverview = New-ITGlueFlexibleAssets -organization_id $ITG_ID -data $FlexAssetBody
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
					$ITG_Device = $ITG_Devices | Where-Object { $_.id -eq $Device.DeviceID }

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

Read-Host "Press ENTER to close..." 