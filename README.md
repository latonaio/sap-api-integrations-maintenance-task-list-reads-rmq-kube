# sap-api-integrations-maintenance-task-list-reads-rmq-kube  
sap-api-integrations-maintenance-task-list-reads-rmq-kube は、外部システム(特にエッジコンピューティング環境)をSAPと統合することを目的に、SAP API で 保全タスクリスト データを取得するマイクロサービスです。  
sap-api-integrations-maintenance-task-list-reads-rmq-kube には、サンプルのAPI Json フォーマットが含まれています。  
sap-api-integrations-maintenance-task-list-reads-rmq-kube は、オンプレミス版である（＝クラウド版ではない）SAPS4HANA API の利用を前提としています。クラウド版APIを利用する場合は、ご注意ください。  
https://api.sap.com/api/OP_API_MAINTENANCETASKLIST_0001/overview  

## 動作環境
sap-api-integrations-maintenance-task-list-reads-rmq-kube は、主にエッジコンピューティング環境における動作にフォーカスしています。   
使用する際は、事前に下記の通り エッジコンピューティングの動作環境（推奨/必須）を用意してください。   
・ エッジ Kubernetes （推奨）    
・ AION のリソース （推奨)    
・ OS: LinuxOS （必須）    
・ CPU: ARM/AMD/Intel（いずれか必須）   
・ RabbitMQ on Kubernetes  
・ RabbitMQ Client         　

## クラウド環境での利用  
sap-api-integrations-maintenance-task-list-reads-rmq-kube は、外部システムがクラウド環境である場合にSAPと統合するときにおいても、利用可能なように設計されています。  

## RabbitMQ からの JSON Input

sap-api-integrations-maintenance-task-list-reads-rmq-kube は、Inputとして、RabbitMQ からのメッセージをJSON形式で受け取ります。 
Input の サンプルJSON は、Inputs フォルダ内にあります。  

## RabbitMQ からのメッセージ受信による イベントドリヴン の ランタイム実行

sap-api-integrations-maintenance-task-list-reads-rmq-kube は、RabbitMQ からのメッセージを受け取ると、イベントドリヴンでランタイムを実行します。  
AION の仕様では、Kubernetes 上 の 当該マイクロサービスPod は 立ち上がったまま待機状態で当該メッセージを受け取り、（コンテナ起動などの段取時間をカットして）即座にランタイムを実行します。　

## RabbitMQ への JSON Output

sap-api-integrations-maintenance-task-list-reads-rmq-kube は、Outputとして、RabbitMQ へのメッセージをJSON形式で出力します。  
Output の サンプルJSON は、Outputs フォルダ内にあります。  

## RabbitMQ の マスタサーバ環境

sap-api-integrations-maintenance-task-list-reads-rmq-kube が利用する RabbitMQ のマスタサーバ環境は、[rabbitmq-on-kubernetes](https://github.com/latonaio/rabbitmq-on-kubernetes) です。  
当該マスタサーバ環境は、同じエッジコンピューティングデバイスに配置されても、別の物理(仮想)サーバ内に配置されても、どちらでも構いません。

## RabbitMQ の Golang Runtime ライブラリ
sap-api-integrations-maintenance-task-list-reads-rmq-kube は、RabbitMQ の Golang Runtime ライブラリ として、[rabbitmq-golang-client](https://github.com/latonaio/rabbitmq-golang-client)を利用しています。

## デプロイ・稼働
sap-api-integrations-maintenance-task-list-reads-rmq-kube の デプロイ・稼働 を行うためには、aion-service-definitions の services.yml に、本レポジトリの services.yml を設定する必要があります。

kubectl apply - f 等で Deployment作成後、以下のコマンドで Pod が正しく生成されていることを確認してください。
```
$ kubectl get pods
```

## 本レポジトリ が 対応する API サービス
sap-api-integrations-maintenance-task-list-reads-rmq-kube が対応する APIサービス は、次のものです。

* APIサービス概要説明 URL: https://api.sap.com/api/OP_API_MAINTENANCETASKLIST_0001/overview  
* APIサービス名(=baseURL): api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001

## 本レポジトリ に 含まれる API名
sap-api-integrations-maintenance-task-list-reads-rmq-kube には、次の API をコールするためのリソースが含まれています。  

* MaintenanceTaskList（保全タスクリスト - ヘッダ）
* MaintenanceTaskListStrtgyPckg（保全タスクリスト - 方針パッケージ）
* MaintenanceTaskListOperation（保全タスクリスト - 作業）
* MaintenanceTaskListOpMat（保全タスクリスト - 作業品目）

## API への 値入力条件 の 初期値
sap-api-integrations-maintenance-task-list-reads-rmq-kube において、API への値入力条件の初期値は、入力ファイルレイアウトの種別毎に、次の通りとなっています。  

## SDC レイアウト

* inoutSDC.MaintenanceTaskList.TaskListType（タスクリストタイプ）
* inoutSDC.MaintenanceTaskList.TaskListGroup（タスクリストグループ）
* inoutSDC.MaintenanceTaskList.TaskListGroupCounter（タスクリストグループカウンタ）
* inoutSDC.MaintenanceTaskList.TaskListVersionCounter（タスクリストバージョンカウンタ）
* inoutSDC.MaintenanceTaskList.Equipment（設備）
* inoutSDC.MaintenanceTaskList.Plant（プラント）
* inoutSDC.MaintenanceTaskList.StrategyPackage.TaskListSequence（タスクリスト順序）
* inoutSDC.MaintenanceTaskList.StrategyPackage.MaintenancePackageText（保全パッケージテキスト）
* inoutSDC.MaintenanceTaskList.StrategyPackage.Operation.TechnicalObject（技術対象）
* inoutSDC.MaintenanceTaskList.StrategyPackage.Operation.OperationText（作業テキスト）

## SAP API Bussiness Hub の API の選択的コール

Latona および AION の SAP 関連リソースでは、Inputs フォルダ下の sample.json の accepter に取得したいデータの種別（＝APIの種別）を入力し、指定することができます。  
なお、同 accepter にAll(もしくは空白)の値を入力することで、全データ（＝全APIの種別）をまとめて取得することができます。  

* sample.jsonの記載例(1)  

accepter において 下記の例のように、データの種別（＝APIの種別）を指定します。  
ここでは、"Header" が指定されています。    
  
```
	"api_schema": "sap.s4.beh.maintenancetasklist.v1.MaintenanceTaskList.Created.v1",
	"accepter": ["Header"],
	"maintenance_task_list": "1",
	"deleted": false
```
  
* 全データを取得する際のsample.jsonの記載例(2)  

全データを取得する場合、sample.json は以下のように記載します。  

```
	"api_schema": "sap.s4.beh.maintenancetasklist.v1.MaintenanceTaskList.Created.v1",
	"accepter": ["All"],
	"maintenance_task_list": "1",
	"deleted": false
```

## 指定されたデータ種別のコール

accepter における データ種別 の指定に基づいて SAP_API_Caller 内の caller.go で API がコールされます。  
caller.go の func() 毎 の 以下の箇所が、指定された API をコールするソースコードです。  

```
func (c *SAPAPICaller) AsyncGetMaintenanceTaskList(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter, equipment, plant, taskListSequence, maintenancePackageText, technicalObject, operationText string, accepter []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(accepter))
	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				c.Header(taskListType, taskListGroup, taskListGroupCounter, taskListVersionCounter)
				wg.Done()
			}()
		case "HeaderEquipmentPlant":
			func() {
				c.HeaderEquipmentPlant(equipment, plant)
				wg.Done()
			}()
		case "StrategyPackage":
			func() {
				c.StrategyPackage(taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
				wg.Done()
			}()
		case "StrategyPackageText":
			func() {
				c.StrategyPackageText(maintenancePackageText)
				wg.Done()
			}()
		case "Operation":
			func() {
				c.Operation(plant, technicalObject)
				wg.Done()
			}()
		case "OperationText":
			func() {
				c.OperationText(plant, operationText)
				wg.Done()
			}()
		case "OperationMaterial":
			func() {
				c.OperationMaterial(taskListType, taskListGroup, taskListGroupCounter, taskListSequence)
				wg.Done()
			}()
		default:
			wg.Done()
		}
	}

	wg.Wait()
}
```

## SAP API Business Hub における API サービス の バージョン と バージョン におけるデータレイアウトの相違

SAP API Business Hub における API サービス のうちの 殆どの API サービス のBASE URLのフォーマットは、"API_(リポジトリ名)_SRV" であり、殆どの API サービス 間 の データレイアウトは統一されています。   
従って、Latona および AION における リソースにおいても、データレイアウトが統一されています。    
一方、本レポジトリ に関わる API である Maintenance Task List のサービスは、BASE URLのフォーマットが他のAPIサービスと異なります。      
その結果、本レポジトリ内の一部のAPIのデータレイアウトが、他のAPIサービスのものと異なっています。  

#### BASE URLが "API_(リポジトリ名)_SRV" のフォーマットである API サービス の データレイアウト（=responses）  
BASE URLが "API_{リポジトリ名}_SRV" のフォーマットであるAPIサービスのデータレイアウト（=responses）は、例えば、次の通りです。  
```
type ToProductionOrderItem struct {
	D struct {
		Results []struct {
			Metadata struct {
				ID   string `json:"id"`
				URI  string `json:"uri"`
				Type string `json:"type"`
			} `json:"__metadata"`
			ManufacturingOrder             string      `json:"ManufacturingOrder"`
			ManufacturingOrderItem         string      `json:"ManufacturingOrderItem"`
			ManufacturingOrderCategory     string      `json:"ManufacturingOrderCategory"`
			ManufacturingOrderType         string      `json:"ManufacturingOrderType"`
			IsCompletelyDelivered          bool        `json:"IsCompletelyDelivered"`
			Material                       string      `json:"Material"`
			ProductionPlant                string      `json:"ProductionPlant"`
			Plant                          string      `json:"Plant"`
			MRPArea                        string      `json:"MRPArea"`
			QuantityDistributionKey        string      `json:"QuantityDistributionKey"`
			MaterialGoodsReceiptDuration   string      `json:"MaterialGoodsReceiptDuration"`
			StorageLocation                string      `json:"StorageLocation"`
			Batch                          string      `json:"Batch"`
			InventoryUsabilityCode         string      `json:"InventoryUsabilityCode"`
			GoodsRecipientName             string      `json:"GoodsRecipientName"`
			UnloadingPointName             string      `json:"UnloadingPointName"`
			MfgOrderItemPlndDeliveryDate   string      `json:"MfgOrderItemPlndDeliveryDate"`
			MfgOrderItemActualDeliveryDate string      `json:"MfgOrderItemActualDeliveryDate"`
			ProductionUnit                 string      `json:"ProductionUnit"`
			MfgOrderItemPlannedTotalQty    string      `json:"MfgOrderItemPlannedTotalQty"`
			MfgOrderItemPlannedScrapQty    string      `json:"MfgOrderItemPlannedScrapQty"`
			MfgOrderItemGoodsReceiptQty    string      `json:"MfgOrderItemGoodsReceiptQty"`
			MfgOrderItemActualDeviationQty string      `json:"MfgOrderItemActualDeviationQty"`
		} `json:"results"`
	} `json:"d"`
}

```

#### BASE URL が "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001" である Maintenance Task List の APIサービス の データレイアウト（=responses）  
BASE URL が "api_maintenancetasklist/srvd_a2x/sap/maintenancetasklist/0001" である Maintenance Task Listの APIサービス の データレイアウト（=responses）は、例えば、次の通りです。  

```
type Header struct {
	Value             []struct {
		TaskListType                string        `json:"TaskListType"`
		TaskListGroup               string        `json:"TaskListGroup"`
		TaskListGroupCounter        string        `json:"TaskListGroupCounter"`
		TaskListVersionCounter      string        `json:"TaskListVersionCounter"`
		MaintenancePlanningPlant    string        `json:"MaintenancePlanningPlant"`
		TaskListStatus              string        `json:"TaskListStatus"`
		TaskListStatusDesc          string        `json:"TaskListStatusDesc"`
		TechnicalObject             string        `json:"TechnicalObject"`
		TaskListUsage               string        `json:"TaskListUsage"`
		TaskListDesc                string        `json:"TaskListDesc"`
		WorkCenter                  string        `json:"WorkCenter"`
		MaintenanceStrategy         string        `json:"MaintenanceStrategy"`
		OperationSystemCondition    string        `json:"OperationSystemCondition"`
		Assembly                    string        `json:"Assembly"`
		ChangeNumber                string        `json:"ChangeNumber"`
		ValidityStartDate           string        `json:"ValidityStartDate"`
		ValidityEndDate             string        `json:"ValidityEndDate"`
		IsMarkedForDeletion         bool          `json:"IsMarkedForDeletion"`
		LastChangeDate              string        `json:"LastChangeDate"`
		CreationDate                string        `json:"CreationDate"`
		Plant                       string        `json:"Plant"`
		ResponsiblePlannerGroup     string        `json:"ResponsiblePlannerGroup"`
		Equipment                   string        `json:"Equipment"`
		FunctionalLocationLabelName string        `json:"FunctionalLocationLabelName"`
		TaskListIsHierarchical      bool          `json:"TaskListIsHierarchical"`
	} `json:"value"`
}
```
このように、BASE URLが "API_(リポジトリ名)_SRV" のフォーマットである API サービス の データレイアウトと、 Maintenance Task List の データレイアウトは、D、Results、Metadata、Value の配列構造を持っているか持っていないかという点が異なります。  

## Output  
本マイクロサービスでは、[golang-logging-library](https://github.com/latonaio/golang-logging-library) により、以下のようなデータがJSON形式で出力されます。  
以下の sample.json の例は、SAP 保全タスクリスト の ヘッダデータ が取得された結果の JSON の例です。  
以下の項目のうち、"TaskListType" ～ "TaskListIsHierarchical" は、/SAP_API_Output_Formatter/type.go 内 の Type Header {} による出力結果です。"cursor" ～ "time"は、golang-logging-library による 定型フォーマットの出力結果です。  

```
{
	"cursor": "/Users/latona2/bitbucket/sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Caller/caller.go#L83",
	"function": "sap-api-integrations-maintenance-task-list-reads-rmq-kube/SAP_API_Caller.(*SAPAPICaller).Header",
	"level": "INFO",
	"message": [
		{
			"TaskListType": "A",
			"TaskListGroup": "1",
			"TaskListGroupCounter": "1",
			"TaskListVersionCounter": "1",
			"MaintenancePlanningPlant": "1710",
			"TaskListStatus": "4",
			"TaskListStatusDesc": "Released (General)",
			"TechnicalObject": "",
			"TaskListUsage": "4",
			"TaskListDesc": "EM Task list",
			"WorkCenter": "RES-0100",
			"MaintenanceStrategy": "EM_01",
			"OperationSystemCondition": "0",
			"Assembly": "",
			"ChangeNumber": "",
			"ValidityStartDate": "2017-06-26",
			"ValidityEndDate": "",
			"IsMarkedForDeletion": false,
			"LastChangeDate": "2017-06-26",
			"CreationDate": "2017-06-26",
			"Plant": "1710",
			"ResponsiblePlannerGroup": "YB1",
			"Equipment": "",
			"FunctionalLocationLabelName": "",
			"TaskListIsHierarchical": false
		}
	],
	"time": "2022-01-09T10:17:38.859716+09:00"
}

```
