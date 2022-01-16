# sap-api-integrations-purchase-contract-reads-rmq-kube
sap-api-integrations-purchase-contract-reads-rmq-kube は、外部システム(特にエッジコンピューティング環境)をSAPと統合することを目的に、SAP API で　購買基本契約データを取得するマイクロサービスです。    
sap-api-integrations-purchase-contract-reads-rmq-kube には、サンプルのAPI Json フォーマットが含まれています。   
sap-api-integrations-purchase-contract-reads-rmq-kube は、オンプレミス版である（＝クラウド版ではない）SAPS4HANA API の利用を前提としています。クラウド版APIを利用する場合は、ご注意ください。   
https://api.sap.com/api/OP_API_PURCHASECONTRACT_PROCESS_SRV/overview  

## 動作環境  
sap-api-integrations-purchase-contract-reads-rmq-kube は、主にエッジコンピューティング環境における動作にフォーカスしています。  
使用する際は、事前に下記の通り エッジコンピューティングの動作環境（推奨/必須）を用意してください。  
・ エッジ Kubernetes （推奨）    
・ AION のリソース （推奨)    
・ OS: LinuxOS （必須）    
・ CPU: ARM/AMD/Intel（いずれか必須）　　
・ RabbitMQ on Kubernetes  
・ RabbitMQ Client        

## クラウド環境での利用
sap-api-integrations-purchase-contract-reads-rmq-kube は、外部システムがクラウド環境である場合にSAPと統合するときにおいても、利用可能なように設計されています。  

## RabbitMQ からの JSON Input

sap-api-integrations-purchase-contract-reads-rmq-kube は、Inputとして、RabbitMQ からのメッセージをJSON形式で受け取ります。  
Input の サンプルJSON は、Inputs フォルダ内にあります。  

## RabbitMQ からのメッセージ受信による イベントドリヴン の ランタイム実行

sap-api-integrations-purchase-contract-reads-rmq-kube は、RabbitMQ からのメッセージを受け取ると、イベントドリヴンでランタイムを実行します。  
AION の仕様では、Kubernetes 上 の 当該マイクロサービスPod は 立ち上がったまま待機状態で当該メッセージを受け取り、（コンテナ起動などの段取時間をカットして）即座にランタイムを実行します。　

## RabbitMQ への JSON Output

sap-api-integrations-purchase-contract-reads-rmq-kube は、Outputとして、RabbitMQ へのメッセージをJSON形式で出力します。  
Output の サンプルJSON は、Outputs フォルダ内にあります。  

## RabbitMQ の マスタサーバ環境

sap-api-integrations-purchase-contract-reads-rmq-kube が利用する RabbitMQ のマスタサーバ環境は、[rabbitmq-on-kubernetes](https://github.com/latonaio/rabbitmq-on-kubernetes) です。   
当該マスタサーバ環境は、同じエッジコンピューティングデバイスに配置されても、別の物理(仮想)サーバ内に配置されても、どちらでも構いません。  

## RabbitMQ の Golang Runtime ライブラリ

sap-api-integrations-purchase-contract-reads-rmq-kube は、RabbitMQ の Golang Runtime ライブラリ として、[rabbitmq-golang-client](https://github.com/latonaio/rabbitmq-golang-client)を利用しています。  

## デプロイ・稼働

sap-api-integrations-purchase-contract-reads-rmq-kube の デプロイ・稼働 を行うためには、aion-service-definitions の services.yml に、本レポジトリの services.yml を設定する必要があります。  

kubectl apply - f 等で Deployment作成後、以下のコマンドで Pod が正しく生成されていることを確認してください。  
```
$ kubectl get pods
```


## 本レポジトリ が 対応する API サービス
sap-api-integrations-purchase-contract-reads-rmq-kube が対応する APIサービス は、次のものです。

* APIサービス概要説明 URL: https://api.sap.com/api/OP_API_PURCHASECONTRACT_PROCESS_SRV/overview    
* APIサービス名(=baseURL): API_PURCHASE_CONTRACT_SRV

## 本レポジトリ に 含まれる API名
sap-api-integrations-purchase-contract-reads-rmq-kube には、次の API をコールするためのリソースが含まれています。  

* A_PurchaseContract（購買基本契約 - ヘッダ）※購買基本契約の詳細データを取得するために、ToHeaderPartner、ToItem、ToItemCondition、ToItemAddress、と合わせて利用されます。
* A_PurchaseContractItem（購買基本契約 - 明細）※購買基本契約明細の詳細データを取得するために、ToItemCondition、ToItemAddress、と合わせて利用されます。
* ToItem（購買基本契約 - 明細）
* ToItemCondition（購買基本契約 - 明細価格条件）
* ToItemAddress（購買基本契約 - 明細住所）

## API への 値入力条件 の 初期値
sap-api-integrations-purchase-contract-reads-rmq-kube において、API への値入力条件の初期値は、入力ファイルレイアウトの種別毎に、次の通りとなっています。  

### SDC レイアウト

* inoutSDC.PurchaseContract.PurchaseContract（購買基本契約番号）
* inoutSDC.PurchaseContract.PurchaseContractItem.PurchaseContractItem（購買基本契約明細）

## SAP API Bussiness Hub の API の選択的コール

Latona および AION の SAP 関連リソースでは、Inputs フォルダ下の sample.json の accepter に取得したいデータの種別（＝APIの種別）を入力し、指定することができます。  
なお、同 accepter にAll(もしくは空白)の値を入力することで、全データ（＝全APIの種別）をまとめて取得することができます。  

* sample.jsonの記載例(1)  

accepter において 下記の例のように、データの種別（＝APIの種別）を指定します。  
ここでは、"Header" が指定されています。

```
	"api_schema": "sap.s4.beh.purchasecontract.v1.PurchaseContract.Created.v1",
	"accepter": ["Header"],
	"purchase_contract": "4600000001",
	"deleted": false
```
  
* 全データを取得する際のsample.jsonの記載例(2)  

全データを取得する場合、sample.json は以下のように記載します。  

```
	"api_schema": "sap.s4.beh.purchasecontract.v1.PurchaseContract.Created.v1",
	"accepter": ["All"],
	"purchase_contract": "4600000001",
	"deleted": false
```

## 指定されたデータ種別のコール

accepter における データ種別 の指定に基づいて SAP_API_Caller 内の caller.go で API がコールされます。  
caller.go の func() 毎 の 以下の箇所が、指定された API をコールするソースコードです。  

```
func (c *SAPAPICaller) AsyncGetPurchaseContract(purchaseContract, purchaseContractItem string, accepter []string) {
	wg := &sync.WaitGroup{}
	wg.Add(len(accepter))
	for _, fn := range accepter {
		switch fn {
		case "Header":
			func() {
				c.Header(purchaseContract)
				wg.Done()
			}()
		case "Item":
			func() {
				c.Item(purchaseContract, purchaseContractItem)
				wg.Done()
			}()
		default:
			wg.Done()
		}
	}

	wg.Wait()
}
```

## Output  
本マイクロサービスでは、[golang-logging-library](https://github.com/latonaio/golang-logging-library) により、以下のようなデータがJSON形式で出力されます。  
以下の sample.json の例は、SAP 購買基本契約 の ヘッダデータ が取得された結果の JSON の例です。  
以下の項目のうち、"SalesContract" ～ "to_Item" は、/SAP_API_Output_Formatter/type.go 内 の Type Product {} による出力結果です。"cursor" ～ "time"は、golang-logging-library による 定型フォーマットの出力結果です。  

```
{
	"cursor": "/Users/latona2/bitbucket/sap-api-integrations-purchase-contract-reads/SAP_API_Caller/caller.go#L58",
	"function": "sap-api-integrations-purchase-contract-reads/SAP_API_Caller.(*SAPAPICaller).Header",
	"level": "INFO",
	"message": [
		{
			"PurchaseContract": "4600000001",
			"PurchaseContractType": "MK",
			"CompanyCode": "1710",
			"PurchasingDocumentDeletionCode": "",
			"CreationDate": "/Date(1471305600000)/",
			"Supplier": "USSU-VSF10",
			"PurchasingOrganization": "1710",
			"PurchasingGroup": "001",
			"PaymentTerms": "0001",
			"NetPaymentDays": "0",
			"DocumentCurrency": "USD",
			"ExchangeRate": "1.00000",
			"ValidityStartDate": "/Date(1467331200000)/",
			"ValidityEndDate": "/Date(1469923200000)/",
			"SupplierRespSalesPersonName": "",
			"SupplierPhoneNumber": "",
			"IncotermsClassification": "EXW",
			"PurchaseContractTargetAmount": "4000.00",
			"InvoicingParty": "",
			"ReleaseCode": "",
			"IncotermsVersion": "",
			"LastChangeDateTime": "",
			"PurchasingProcessingStatus": "02",
			"PurchasingProcessingStatusName": "Active",
			"PurgContractIsInPreparation": false,
			"to_PurchaseContractItem": "https://sandbox.api.sap.com/s4hanacloud/sap/opu/odata/sap/API_PURCHASECONTRACT_PROCESS_SRV/A_PurchaseContract('4600000001')/to_PurchaseContractItem"
		}
	],
	"time": "2021-12-18T18:46:42.256637+09:00"
}

```
