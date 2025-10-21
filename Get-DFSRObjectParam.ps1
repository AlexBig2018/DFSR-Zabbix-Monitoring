Param(
    #Объект мониторинга, для которого будем возвращать метрики
    [parameter(Mandatory=$true, Position=0)][String]$Object,
    #Дополнительные параметры, на основании которых будет возвращать метрики для объекта
    [parameter(Mandatory=$false, Position=1)]$Param1,
    [parameter(Mandatory=$false, Position=2)]$Param2
)

Set-Variable DFSNamespace -Option ReadOnly -Value "root\MicrosoftDfs" -ErrorAction Ignore
Set-Variable RoleNotInstalledText -Option ReadOnly -Value "DFS Replication role not installed" -ErrorAction Ignore

#Параметры локализации (чтобы разделителем целой и дробной части была точка, т.к. запятую заббикс не поймет)
$USCulture = [System.Globalization.CultureInfo]::GetCultureInfo("en-US")
[System.Threading.Thread]::CurrentThread.CurrentCulture = $USCulture

<#
    Тайм-аут для получения значения (должен быть меньше таймаута в настройках заббикс-агента)
    С помощью этого параметра ограничиваем время опроса партнеров:
    Учитывая, что обычно опрос недоступного партнера занимает до 20с,
    а Timeout для заббикс-агента обычно меньше (3с по умолчанию),
    мы ограничиваем время опроса в самом скрипте, чтобы заббикс-агент не получил NoData из-за таймаута
    Это позволит вместо NoData вернуть значение, либо текст ошибки
#>
Set-Variable RequestTimeout -Option ReadOnly -Value 60 -ErrorAction Ignore

If ($PSVersionTable.PSVersion.Major -lt 3) {
    Write-Output "The script requires PowerShell version 3.0 or above"
    Break
}

# Проверка наличия роли (возвращает $true/$false или $null если Get-WindowsFeature недоступен)
try {
    $DFSRRoleInstalled = (Get-WindowsFeature FS-DFS-Replication -ErrorAction Stop).Installed
} catch {
    # Если команда недоступна (например, на серверной ОС без RSAT), считаем роль установленной по возможности
    $DFSRRoleInstalled = $false
}

$ErrorActionPreference = "Continue"

Switch ($Object) {
    "RoleInstalled" {
        $DFSRRoleInstalled
    }

    "ServiceState" {
        $DFSRService = Get-Service DFSR -ErrorAction SilentlyContinue
        If ($DFSRService) {
            If ($DFSRService.Status -eq 'Running') {
                (Get-WmiObject -Namespace $DFSNamespace -Class DfsrInfo -ErrorAction SilentlyContinue).State
            }
            Else {
                #Если служба остановлена
                [int]100
            }
        }
        Else {
            #Если служба не найдена
            [int]101
        }
    }

    #DFS Replication service version
    "ServiceVer" {
        $DFSRConfig = Get-WmiObject -Namespace $DFSNamespace -Class DfsrConfig -ErrorAction SilentlyContinue
        If ($DFSRConfig) { $DFSRConfig.ServiceVersion } else { "n/a" }
    }

    #DFS Replication provider version
    "ProvVer" {
        $DFSRConfig = Get-WmiObject -Namespace $DFSNamespace -Class DfsrConfig -ErrorAction SilentlyContinue
        If ($DFSRConfig) { $DFSRConfig.ProviderVersion } else { "n/a" }
    }

    #DFS Replication monitoring provider version
    "MonProvVer" {
        $DFSRInfo = Get-WmiObject -Namespace $DFSNamespace -Class DfsrInfo -ErrorAction SilentlyContinue
        If ($DFSRInfo) { $DFSRInfo.ProviderVersion } else { "n/a" }
    }

    "ServiceUptime" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $DFSRInfo = Get-WmiObject -Namespace $DFSNamespace -Class DfsrInfo -ErrorAction SilentlyContinue
        If ($DFSRInfo) {
            $WMIStartTime = $DFSRInfo.ServiceStartTime
            $StartTime = [Management.ManagementDateTimeConverter]::ToDateTime($WMIStartTime)
            (New-TimeSpan -Start $StartTime -End (Get-Date)).TotalSeconds
        } else { "n/a" }
    }

    # Реплицируемая папка
    "RF" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }

        $RFID = $Param1
        if (-not $RFID) { Write-Output "RFID parameter missing"; Break }

        $WMIQuery = "SELECT * FROM DfsrReplicatedFolderConfig WHERE ReplicatedFolderGuid='$RFID'"
        $RFConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        If (!$RFConfig) { Write-Output "RF '$RFID' not found"; Break }

        $RFName = $RFConfig.ReplicatedFolderName
        $RGID = $RFConfig.ReplicationGroupGuid

        #Статистику можно собирать только с Enabled-папок
        If ($RFConfig.Enabled) {
            $WMIQuery = "SELECT * FROM DfsrReplicatedFolderInfo WHERE ReplicatedFolderGuid='$RFID'"
            $RFInfo = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        }
        $ErrorText = "Couldn't retrieve info for disabled RF"

        Switch ($Param2) {
            "Enabled" { $RFConfig.Enabled }
            "RemoveDeleted" { $RFConfig.DisableSaveDeletes }
            "ReadOnly" { $RFConfig.ReadOnly }
            "StageQuota" { $RFConfig.StagingSizeInMb * 1024 * 1024 }
            "ConflictQuota" { $RFConfig.ConflictSizeInMb * 1024 * 1024 }
            "State" {
                If ($RFInfo) { $RFInfo.State } else { $ErrorText }
            }
            "StageSize" {
                If ($RFInfo) { $RFInfo.CurrentStageSizeInMb * 1024 * 1024 } else { $ErrorText }
            }
            "StagePFree" {
                If ($RFInfo) {
                    ($RFConfig.StagingSizeInMb - $RFInfo.CurrentStageSizeInMb) / $RFConfig.StagingSizeInMb * 100
                } else { $ErrorText }
            }
            "ConflictSize" {
                If ($RFInfo) { $RFInfo.CurrentConflictSizeInMb * 1024 * 1024 } else { $ErrorText }
            }
            "ConflictPFree" {
                If ($RFInfo) {
                    ($RFConfig.ConflictSizeInMb - $RFInfo.CurrentConflictSizeInMb) / $RFConfig.ConflictSizeInMb * 100
                } else { $ErrorText }
            }

         "Redundancy" {
    #Write-Host "==== Redundancy check start ===="

    # Находим партнёров по группе репликации
    $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE ReplicationGroupGuid='$RGID'"
    Try {
        $PartnersByGroup = (Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction Stop).PartnerName | Select-Object -Unique
    } Catch {
        #Write-Host "Ошибка при запросе ConnectionConfig: $($_.Exception.Message)"
        Break
    }

    If (-not $PartnersByGroup) {
        #Write-Host "Партнёры для группы '$RGID' не найдены"
        Break
    }

    $n = 0
    #Write-Host "Найдены партнёры: $($PartnersByGroup -join ', ')"

    # Проверяем наличие папки, имеющей состояние 'Normal' (4), на каждом из партнёров
    ForEach ($Partner in $PartnersByGroup) {
        #Write-Host "→ Проверяем $Partner"
        $WMIQuery = "SELECT * FROM DfsrReplicatedFolderInfo WHERE ReplicatedFolderGuid='$RFID' AND State=4"

        Try {
            $result = @(Get-WmiObject -ComputerName $Partner -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction Stop)
            if ($result.Count -gt 0) {
                #Write-Host "[$Partner] найдено $($result.Count) папок в состоянии Normal"
                $n += $result.Count
            }
            else {
                Write-Host "[$Partner] папок в состоянии Normal не найдено"
            }
        }
        Catch {
            #Write-Host "Ошибка при опросе $Partner: $($_.Exception.Message)"
            $n = -1
            Break
        }
    }

    If ($n -ge 0) {
        #Write-Host "==== Redundancy check complete: $n ===="
        $n
    } Else {
        #Write-Host "==== Redundancy check failed ===="
            }
    }
            }
        } 
    "RFBacklog" {
        if (-not $DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; break }

        $RFID = $Param1
        $RServerID = $Param2
        if (-not $RFID -or -not $RServerID) { Write-Output "RFID or RServerID missing"; break }

        $WMIQuery = "SELECT * FROM DfsrReplicatedFolderConfig WHERE ReplicatedFolderGuid='$RFID'"
        $RFConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        if (-not $RFConfig) { Write-Output "RF '$RFID' not found"; break }

        $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE PartnerGuid='$RServerID' AND Inbound='False'"
        $ConnectionConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        if (-not $ConnectionConfig) { Write-Output "Outbound connection to partner '$RServerID' not found"; break }

        $RServerName = $ConnectionConfig.PartnerName
        $WMIQuery = "SELECT * FROM DfsrReplicatedFolderInfo WHERE ReplicatedFolderGuid='$RFID'"

        try {
            if (!(Test-Connection -ComputerName $RServerName -Count 1 -Quiet)) { Write-Output "Partner '$RServerName' is unreachable (ping failed)"; break }

            try {
                $null = Get-WmiObject -ComputerName $RServerName -Namespace "root\cimv2" -Class Win32_OperatingSystem -ErrorAction Stop
            } catch {
                Write-Output "WMI access to '$RServerName' failed: $($_.Exception.Message)"; break
            }

            $j = Get-WmiObject -ComputerName $RServerName -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
            if ($j) {
                try {
                    $VersionVector = $j.GetVersionVector().VersionVector
                    $LocalRF = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
                    $BacklogCount = $LocalRF.GetOutboundBacklogFileCount($VersionVector).BacklogFileCount
                    if ($BacklogCount -ne $null) { $BacklogCount } else { Write-Output "Backlog count not available (null result)" }
                } catch {
                    Write-Output "Error while retrieving backlog info from '$RServerName': $($_.Exception.Message)"
                }
            } else { Write-Output "Partner '$RServerName' returned no DFSR info" }
        }
        catch {
            Write-Output "Unexpected error while processing partner '$RServerName': $($_.Exception.Message)"
        }
    }

    "Connection" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $ConnectionID = $Param1
        $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE ConnectionGuid='$ConnectionID'"
        $ConnectionConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        If (!$ConnectionConfig) { Write-Output "Connection '$ConnectionID' not found"; Break }

        Switch ($Param2) {
            "Enabled" { $ConnectionConfig.Enabled }
            "State" {
                If ($ConnectionConfig.Enabled) {
                    $WMIQuery = "SELECT * FROM DfsrConnectionInfo WHERE ConnectionGuid='$ConnectionID'"
                    $ConnectionInfo = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
                    If ($ConnectionInfo) { $ConnectionInfo.State } else { Write-Output "Couldn't retrieve connection info. Check availability of partner '$($ConnectionConfig.PartnerName)'" }
                } else { Write-Output "Couldn't retrieve info for disabled connection" }
            }
            "BlankSchedule" {
                [Int]$s = 0
                $ConnectionConfig.Schedule | ForEach-Object { $s += $_ }
                [Boolean]($s -eq 0)
            }
        }
    }

    "RG" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $RGID = $Param1
        $WMIQuery = "SELECT * FROM DfsrReplicationGroupConfig WHERE ReplicationGroupGuid='$RGID'"
        $RGConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        If (!$RGConfig) { Write-Output "RG '$RGID' not found"; Break }

        Switch ($Param2) {
            "RFCount" {
                $WMIQuery = "SELECT * FROM DfsrReplicatedFolderConfig WHERE ReplicationGroupGuid='$RGID'"
                @(Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue).Count
            }
            "InConCount" {
                $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE ReplicationGroupGuid='$RGID' AND Inbound='True'"
                @(Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue).Count
            }
            "OutConCount" {
                $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE ReplicationGroupGuid='$RGID' AND Inbound='False'"
                @(Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue).Count
            }
            "BlankSchedule" {
                If ($RGConfig) {
                    [Int]$s = 0
                    $RGConfig.DefaultSchedule | ForEach-Object { $s += $_ }
                    [Boolean]($s -eq 0)
                }
            }
        }
    }

    #Количество групп репликации, в которые входит сервер
    "RGCount" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        @(Get-WmiObject -Namespace $DFSNamespace -Class DfsrReplicationGroupConfig -ErrorAction SilentlyContinue).Count
    }

    "Partner" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $PartnerID = $Param1
        $WMIQuery = "SELECT * FROM DfsrConnectionConfig WHERE PartnerGuid='$PartnerID'"
        $ConnectionConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        If (!$ConnectionConfig) { Write-Output "Partner '$PartnerID' not found"; Break }

        $PartnerName = (Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue).PartnerName | Select-Object -Unique
        Switch ($Param2) {
            "PingCheckOK" {
                $CheckResult = Test-Connection -ComputerName $PartnerName -Count 1 -Delay 1 -ErrorAction SilentlyContinue
                [Boolean]($CheckResult -ne $Null)
            }
            "WMICheckOK" {
                try {
                    $sessionOptions = New-CimSessionOption -Protocol DCOM
                    $session = New-CimSession -ComputerName $PartnerName -SessionOption $sessionOptions

                    $result = Get-CimInstance -CimSession $session -Namespace $DFSNamespace -Query "SELECT * FROM DfsrConfig" -OperationTimeoutSec $RequestTimeout -ErrorAction Stop

                    Remove-CimSession $session

                    if ($null -ne $result) { $true } else { $false }
                } catch {
                    $false
                }
            }
        }
    }

    "Volume" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $VolumeID = $Param1
        $WMIQuery = "SELECT * FROM DfsrVolumeConfig WHERE VolumeGuid='$VolumeID'"
        $VolumeConfig = Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue
        If (!$VolumeConfig) { Write-Output "Volume '$VolumeID' not found"; Break }

        Switch ($Param2) {
            "State" {
                $WMIQuery = "SELECT * FROM DfsrVolumeInfo WHERE VolumeGuid='$VolumeID'"
                (Get-WmiObject -Namespace $DFSNamespace -Query $WMIQuery -ErrorAction SilentlyContinue).State
            }
        }
    }

    "Log" {
        If (!$DFSRRoleInstalled) { Write-Output $RoleNotInstalledText; Break }
        $ErrorActionPreference = "Stop"
        Try {
            [String]$TimeSpanAsString = $Param2
            $EndTime = Get-Date
            $Multiplier = 1
            $TimeUnits = $TimeSpanAsString[$TimeSpanAsString.Length-1]
            If ($TimeUnits -match "\d") {
                $TimeValue = ($TimeSpanAsString -as [Int])
            } else {
                $TimeValue = ($TimeSpanAsString.Substring(0, $TimeSpanAsString.Length - 1) -as [Int])
                Switch ($TimeUnits) {
                    "m" { $Multiplier = 60 }
                    "h" { $Multiplier = 3600 }
                    "d" { $Multiplier = 86400 }
                    "w" { $Multiplier = 604800 }
                }
            }
            $StartTime = $EndTime.AddSeconds(-$TimeValue * $Multiplier)

            $Filter = @{ LogName = "DFS Replication"; StartTime = $StartTime; EndTime = $EndTime }
            Switch ($Param1) {
                "WarnCount" { $Filter += @{ Level = 3 } }
                "ErrCount"  { $Filter += @{ Level = 2 } }
                "CritCount" { $Filter += @{ Level = 1 } }
            }
            @(Get-WinEvent -FilterHashtable $Filter -ErrorAction SilentlyContinue).Count
        } catch {
            $Error[0].Exception.Message
        }
    }
}
