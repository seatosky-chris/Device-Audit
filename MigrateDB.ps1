. "$PSScriptRoot\Config Files\Global-Config.ps1" # Global Config
. "$PSScriptRoot\Config Files\APIKeys.ps1" # API Keys

$CompaniesToAudit = (Get-ChildItem "$PSScriptRoot\Config Files\" | Where-Object { $_.PSIsContainer -eq $false -and $_.Extension -eq '.ps1' -and $_.Name -like "Config-*" }).Name

$CurrentTLS = [System.Net.ServicePointManager]::SecurityProtocol
if ($CurrentTLS -notlike "*Tls12" -and $CurrentTLS -notlike "*Tls13") {
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
	Write-Host "This device is using an old version of TLS. Temporarily changed to use TLS v1.2."
}

# Import/Install any required modules
If (Get-Module -ListAvailable -Name "Az.Accounts") {Import-module Az.Accounts } Else { install-module Az.Accounts  -Force; import-module Az.Accounts }
If (Get-Module -ListAvailable -Name "Az.Resources") {Import-module Az.Resources } Else { install-module Az.Resources  -Force; import-module Az.Resources }
If (Get-Module -ListAvailable -Name "CosmosDB") {Import-module CosmosDB } Else { install-module CosmosDB  -Force; import-module CosmosDB }
If (Get-Module -ListAvailable -Name "PSSQLite") {Import-module PSSQLite } Else { install-module PSSQLite  -Force; import-module PSSQLite }

# Connect to Azure
if (Test-Path "$PSScriptRoot\Config Files\AzureServiceAccount.json") {
	Import-AzContext -Path "$PSScriptRoot\Config Files\AzureServiceAccount.json"
} else {
	Connect-AzAccount
	Save-AzContext -Path "$PSScriptRoot\Config Files\AzureServiceAccount.json"
}

foreach ($ConfigFile in $CompaniesToAudit) {
	. "$PSScriptRoot\Config Files\Global-Config.ps1" # Reimport Global Config to reset anything that was overridden
	. "$PSScriptRoot\Config Files\$ConfigFile" # Import company config
	Write-Host "============================="
	Write-Host "Starting migration for $Company_Acronym" -ForegroundColor Green

	$Database = "$PSScriptRoot/Usage/$($Company_Acronym)_usage.SQLite"
	$SQL_Con = New-SQLiteConnection -DataSource $Database

	$Query = "SELECT * FROM Computers;"
	$Computers = Invoke-SqliteQuery -SQLiteConnection $SQL_Con -Query $Query
	$Query = "SELECT * FROM Users;"
	$Users = Invoke-SqliteQuery -SQLiteConnection $SQL_Con -Query $Query
	$Query = "SELECT * FROM Usage;"
	$Usage = Invoke-SqliteQuery -SQLiteConnection $SQL_Con -Query $Query

	# Connect to Account & DB
	$Account_Name = "stats-$($Company_Acronym.ToLower())"
	$Account = Get-CosmosDbAccount -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup
	if (!$Account) {
		try {
			New-CosmosDbAccount -Name $Account_Name -ResourceGroupName $Database_Connection.ResourceGroup -Location 'WestUS2' -Capability @('EnableServerless')
		} catch { 
			Write-Host "Account creation failed for $Company_Acronym. Exiting..." -ForegroundColor Red
			Read-Host "Press ENTER to close..." 
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
				Read-Host "Press ENTER to close..." 
				exit
			}
		}
	}

	# Turn off indexing on new containers for bulk insert then at end set back to default
	$indexingPolicyNone = New-CosmosDbCollectionIndexingPolicy -Automatic $false -IndexingMode None
	$indexingPolicyDefault = New-CosmosDbCollectionIndexingPolicy -Automatic $true -IndexingMode Consistent

	# Upload data to new DB
	$Now_UTC = Get-Date (Get-Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
	$ComputerIDs = @{}
	$UserIDs = @{}
	$MigrateUsage = $true

	if ($Computers) {
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Computers"
			Write-Host "Skipping migration of computers." -ForegroundColor 'Yellow'
			$MigrateUsage = $false
		} catch {
			try {
				# Create computers collection
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Computers" -PartitionKey "type" -IndexingPolicy $indexingPolicyNone | Out-Null
			} catch {
				$MigrateUsage = $false
				Write-Host "Table creation of 'Computers' failed. Exiting..." -ForegroundColor Red
				Read-Host "Press ENTER to close..." 
				exit
			}

			# Migrate data
			Write-Progress -Activity "Migrating Computers" -Status "Starting" -PercentComplete 0
			$total = ($Computers | Measure-Object).Count
			$i = 0
			foreach ($Computer in $Computers) {
				$i++
				$ComputerID = $([Guid]::NewGuid().ToString())
				$NewComputer = @{
					id = $ComputerID
					Hostname = $Computer.Hostname
					SC_ID = $Computer.SC_ID
					RMM_ID = $Computer.RMM_ID
					Sophos_ID = $Computer.Sophos_ID
					DeviceType = $Computer.DeviceType
					SerialNumber = $Computer.SerialNumber
					Manufacturer = $Computer.Manufacturer
					Model = $Computer.Model
					OS = $Computer.OS
					WarrantyExpiry = if ($Computer.WarrantyExpiry) { ([DateTime]$Computer.WarrantyExpiry).ToString("yyyy-MM-ddT00:00:00.000Z") } else { $null }
					LastUpdated = $Now_UTC
					type = "computer"
				} | ConvertTo-Json
				Write-Progress -Activity "Migrating Computers" -Status "In Progress - Migrating computer '$($Computer.Hostname)'" -PercentComplete (($i/$total)*100)
				New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Computers" -DocumentBody $NewComputer -PartitionKey 'computer' | Out-Null
				$ComputerIDs[$Computer.ComputerID] = $ComputerID
			}
			Write-Progress -Activity "Migrating Computers" -Status "Complete" -PercentComplete 100
			Set-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Computers" -IndexingPolicy $indexingPolicyDefault
			Write-Host "Created new table and migrated data: Computers" -ForegroundColor Green
		}
	}

	if ($Users) {
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Users"
			Write-Host "Skipping migration of users." -ForegroundColor 'Yellow'
			$MigrateUsage = $false
		} catch {
			try {
				# Create users collection
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Users" -PartitionKey "type" -IndexingPolicy $indexingPolicyNone | Out-Null
			} catch {
				$MigrateUsage = $false
				Write-Host "Table creation of 'Users' failed. Exiting..." -ForegroundColor Red
				Read-Host "Press ENTER to close..." 
				exit
			}

			# Migrate data
			Write-Progress -Activity "Migrating Users" -Status "Starting" -PercentComplete 0
			$total = ($Users | Measure-Object).Count
			$i = 0
			foreach ($User in $Users) {
				$i++
				$UserID = $([Guid]::NewGuid().ToString())
				$NewUser = @{
					id = $UserID
					Username = $User.Username
					DomainOrLocal = ""
					Domain = ""
					ADUsername = $null
					O365Email = $null
					ITG_ID = $null
					LastUpdated = $Now_UTC
					type = "user"
				} | ConvertTo-Json
				Write-Progress -Activity "Migrating Users" -Status "In Progress - Migrating user '$($User.Username)'" -PercentComplete (($i/$total)*100)
				New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Users" -DocumentBody $NewUser -PartitionKey 'user' | Out-Null
				$UserIDs[$User.UserID] = $UserID
			}
			Write-Progress -Activity "Migrating Users" -Status "Complete" -PercentComplete 100
			Set-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Users" -IndexingPolicy $indexingPolicyDefault
			Write-Host "Created new table and migrated data: Users" -ForegroundColor Green
		}
	}

	if ($Usage -and $MigrateUsage) {
		try {
			Get-CosmosDbCollection -Context $cosmosDbContext -Id "Usage"
			Write-Host "Skipping migration of usage." -ForegroundColor 'Yellow'
		} catch {
			try {
				# Create usage collection
				New-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Usage" -PartitionKey "yearmonth" -IndexingPolicy $indexingPolicyNone | Out-Null
			} catch {
				Write-Host "Table creation of 'Usage' failed. Exiting..." -ForegroundColor Red
				Read-Host "Press ENTER to close..." 
				exit
			}

			# Migrate data
			Write-Progress -Activity "Migrating Usage" -Status "Starting" -PercentComplete 0
			$total = ($Usage | Measure-Object).Count
			$i = 0
			foreach ($Use in $Usage) {
				$i++
				$ID = $([Guid]::NewGuid().ToString())
				$Year_Month = Get-Date $Use.Date -Format 'yyyy-MM'
				$NewUse = @{
					id = $ID
					ComputerID = $ComputerIDs[$Use.ComputerID]
					UserID = $UserIDs[$Use.UserID]
					UseDateTime = Get-Date (Get-Date $Use.Date).ToUniversalTime() -UFormat '+%Y-%m-%dT%H:%M:%S.000Z'
					yearmonth = $Year_Month
				} | ConvertTo-Json
				Write-Progress -Activity "Migrating Usage" -Status "In Progress - '$Year_Month'" -PercentComplete (($i/$total)*100)
				New-CosmosDbDocument -Context $cosmosDbContext -Database $DB_Name -CollectionId "Usage" -DocumentBody $NewUse -PartitionKey $Year_Month | Out-Null
			}
			Write-Progress -Activity "Migrating Usage" -Status "Complete" -PercentComplete 100
			Set-CosmosDbCollection -Context $cosmosDbContext -Database $DB_Name -Id "Usage" -IndexingPolicy $indexingPolicyDefault
			Write-Host "Created new table and migrated data: Usage" -ForegroundColor Green
		}
	} else {
		Write-Host "Could not migrate usage." -ForegroundColor Red
	}
}