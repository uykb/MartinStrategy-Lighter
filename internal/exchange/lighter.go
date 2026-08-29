package exchange

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/elliottech/lighter-go/client"
	lighterHttp "github.com/elliottech/lighter-go/client/http"
	"github.com/elliottech/lighter-go/types"
	"github.com/uykb/MartinStrategy/internal/config"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// LighterConfig 封装 Lighter 交易所的配置参数
type LighterConfig struct {
	APIURL         string
	WSURL          string
	PrivateKey     string // API key private key (hex)
	AccountIndex   int64  // Account index
	ApiKeyIndex    uint8  // API key index (usually 2-254)
	Symbol         string // Configured symbol
	UseTestnet     bool
	ChainID        uint32
	PingInterval   time.Duration
	MaxReconnect   int
	InitialBackoff time.Duration
}

// NewLighterConfig 从通用 ExchangeConfig 创建 Lighter 专属配置
func NewLighterConfig(cfg *config.ExchangeConfig) (*LighterConfig, error) {
	var chainID uint32 = 304
	apiURL := "https://mainnet.zklighter.elliot.ai"
	wsURL := "wss://mainnet.zklighter.elliot.ai/stream"

	if cfg.UseTestnet {
		chainID = 300
		apiURL = "https://testnet.zklighter.elliot.ai"
		wsURL = "wss://testnet.zklighter.elliot.ai/stream"
	}

	var accountIndex int64
	if strings.HasPrefix(cfg.Account, "0x") {
		index, err := lookupAccountIndex(apiURL, cfg.Account)
		if err != nil {
			return nil, fmt.Errorf("failed to lookup account index for L1 address %s: %w", cfg.Account, err)
		}
		accountIndex = index
	} else {
		index, err := strconv.ParseInt(cfg.Account, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid account (must be integer account index or hex wallet address): %w", err)
		}
		accountIndex = index
	}

	apiKeyIndex := cfg.ApiKeyIndex

	return &LighterConfig{
		APIURL:         apiURL,
		WSURL:          wsURL,
		PrivateKey:     cfg.ApiKey,
		AccountIndex:   accountIndex,
		ApiKeyIndex:    apiKeyIndex,
		Symbol:         cfg.Symbol,
		UseTestnet:     cfg.UseTestnet,
		ChainID:        chainID,
		PingInterval:   30 * time.Second,
		MaxReconnect:   10,
		InitialBackoff: 2 * time.Second,
	}, nil
}

// lookupAccountIndex fetches all accounts for a wallet from the Lighter API and returns the first one
func lookupAccountIndex(apiURL, walletAddr string) (int64, error) {
	endpoint := fmt.Sprintf("%s/api/v1/accountsByL1Address?l1_address=%s", apiURL, walletAddr)
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("status %d: %s", resp.StatusCode, string(body))
	}

	var accountResp struct {
		Code        int    `json:"code"`
		Message     string `json:"message"`
		SubAccounts []struct {
			Index int64 `json:"index"`
		} `json:"sub_accounts"`
	}
	if err := json.Unmarshal(body, &accountResp); err != nil {
		return 0, err
	}

	if accountResp.Code != 200 {
		return 0, fmt.Errorf("API error code %d: %s", accountResp.Code, accountResp.Message)
	}

	if len(accountResp.SubAccounts) == 0 {
		return 0, fmt.Errorf("no sub accounts found for wallet %s", walletAddr)
	}

	return accountResp.SubAccounts[0].Index, nil
}

// LighterAccountResponse Lighter detailed account structure for parsing responses
type LighterAccountResponse struct {
	Code     int                      `json:"code"`
	Message  string                   `json:"message"`
	Total    int64                    `json:"total"`
	Accounts []LighterDetailedAccount `json:"accounts"`
}

type LighterDetailedAccount struct {
	Index            int64                    `json:"index"`
	L1Address        string                   `json:"l1_address"`
	AvailableBalance string                   `json:"available_balance"`
	Collateral       string                   `json:"collateral"`
	Positions        []LighterAccountPosition `json:"positions"`
	Assets           []LighterAccountAsset    `json:"assets"`
}

type LighterAccountPosition struct {
	MarketID              int16  `json:"market_id"`
	Symbol                string `json:"symbol"`
	Sign                  int32  `json:"sign"` // 1 = long, -1 = short
	Position              string `json:"position"`
	AvgEntryPrice         string `json:"avg_entry_price"`
	PositionValue         string `json:"position_value"`
	UnrealizedPnl         string `json:"unrealized_pnl"`
	LiquidationPrice      string `json:"liquidation_price"`
	InitialMarginFraction string `json:"initial_margin_fraction"`
	AllocatedMargin       string `json:"allocated_margin"`
}

type LighterAccountAsset struct {
	Symbol        string `json:"symbol"`
	AssetID       int16  `json:"asset_id"`
	Balance       string `json:"balance"`
	LockedBalance string `json:"locked_balance"`
}

// LighterAdapter 实现 ExchangeAdapter 接口
type LighterAdapter struct {
	localOb  *Orderbook
	cfg        *LighterConfig
	bus        *core.EventBus
	txClient   *client.TxClient
	httpClient *http.Client
	wsManager  *WSManager

	symbolInfo   *SymbolInfo
	symbolInfoMu sync.RWMutex

	timeOffset int64

	ctx    context.Context
	cancel context.CancelFunc
}

// NewLighterAdapter 创建 Lighter 适配器实例
func (l *LighterAdapter) GetLocalOrderbook() *Orderbook { return l.localOb }

func NewLighterAdapter(cfg *config.ExchangeConfig, bus *core.EventBus) (*LighterAdapter, error) {
	lCfg, err := NewLighterConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("Lighter config error: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	utils.Logger.Info("Lighter 适配器初始化",
		zap.String("symbol", lCfg.Symbol),
		zap.Bool("testnet", lCfg.UseTestnet),
		zap.Int64("account_index", lCfg.AccountIndex),
		zap.Uint8("api_key_index", lCfg.ApiKeyIndex))

	// Create MinimalHTTPClient
	minimalClient := lighterHttp.NewClient(lCfg.APIURL)

	// Create TxClient
	txClient, err := client.NewTxClient(minimalClient, lCfg.PrivateKey, lCfg.AccountIndex, lCfg.ApiKeyIndex, lCfg.ChainID)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create Lighter TxClient: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 30 * time.Second,
	}

	adapter := &LighterAdapter{
		cfg:        lCfg,
		bus:        bus,
		txClient:   txClient,
		httpClient: httpClient,
		ctx:        ctx,
		cancel:     cancel,
		localOb:    NewOrderbook(),
	}

	adapter.wsManager = NewWSManager(lCfg, bus, adapter)

	return adapter, nil
}

// Start 启动适配器：获取交易对精度信息 + 启动 WebSocket + 启动 REST 降级
func (l *LighterAdapter) syncServerTime() {
	resp, err := http.Head(fmt.Sprintf("%s/api/v1/orderBooks", l.cfg.APIURL))
	if err == nil {
		if dateStr := resp.Header.Get("Date"); dateStr != "" {
			if st, err := time.Parse(time.RFC1123, dateStr); err == nil {
				l.timeOffset = st.UnixMilli() - time.Now().UnixMilli()
				utils.Logger.Info("已同步服务器时间偏移", zap.Int64("offset_ms", l.timeOffset))
			}
		}
	}
}

func (l *LighterAdapter) Start(ctx context.Context) error {
	if err := l.initSymbolInfo(); err != nil {
		return fmt.Errorf("初始化交易对精度失败: %w", err)
	}

	l.syncServerTime()

	utils.Logger.Info("Lighter 适配器启动",
		zap.String("symbol", l.cfg.Symbol),
		zap.String("api_url", l.cfg.APIURL),
		zap.String("ws_url", l.cfg.WSURL))

	if err := l.wsManager.Start(); err != nil {
		return fmt.Errorf("启动 WebSocket 失败: %w", err)
	}

	go l.restPriceFallback()

	return nil
}

// Stop 关闭适配器
func (l *LighterAdapter) Stop() error {
	utils.Logger.Info("Lighter 适配器正在关闭...")
	l.wsManager.Stop()
	l.cancel()
	utils.Logger.Info("Lighter 适配器已关闭")
	return nil
}

func (l *LighterAdapter) sendGetRequest(endpoint string) ([]byte, error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	// ★ 官方文档要求：对每个请求进行认证以绕过 IP 速率限制。
	// "To bypass IP-based rate limits, clients can authenticate each request so that only L1-based rate limits apply."
	// 无认证的机房 IP 请求会触发 AWS WAF 的 Human Verification 人机验证挑战（HTTP 405）。
	token, err := l.getAuthToken()
	if err != nil {
		utils.Logger.Warn("生成认证 Token 失败，请求将以匿名方式发送", zap.Error(err))
	} else {
		req.Header.Set("authorization", token)
	}

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

// GetLatestPrice 从 REST 获取最新价格
func (l *LighterAdapter) GetLatestPrice() (float64, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return 0, err
	}

	endpoint := fmt.Sprintf("%s/api/v1/orderBookDetails?market_id=%d", l.cfg.APIURL, marketInfo.MarketID)
	body, err := l.sendGetRequest(endpoint)
	if err != nil {
		return 0, err
	}

	var apiResp struct {
		Code             int `json:"code"`
		OrderBookDetails []struct {
			Symbol         string  `json:"symbol"`
			LastTradePrice float64 `json:"last_trade_price"`
		} `json:"order_book_details"`
		SpotOrderBookDetails []struct {
			Symbol         string  `json:"symbol"`
			LastTradePrice float64 `json:"last_trade_price"`
		} `json:"spot_order_book_details"`
	}

	if err := json.Unmarshal(body, &apiResp); err != nil {
		return 0, err
	}

	normSymbol := normalizeSymbol(l.cfg.Symbol)

	// Search perp markets
	for _, o := range apiResp.OrderBookDetails {
		if normalizeSymbol(o.Symbol) == normSymbol {
			return o.LastTradePrice, nil
		}
	}

	// Search spot markets
	for _, o := range apiResp.SpotOrderBookDetails {
		if normalizeSymbol(o.Symbol) == normSymbol {
			return o.LastTradePrice, nil
		}
	}

	return 0, fmt.Errorf("market symbol %s not found in orderBookDetails", l.cfg.Symbol)
}

// GetKlines 获取 K 线数据
func (l *LighterAdapter) GetKlines(interval string, limit int) ([]Candle, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return nil, err
	}

	endTime := time.Now().UnixMilli()
	duration := intervalToDuration(interval)
	startTime := endTime - int64(limit)*duration.Milliseconds()

	endpoint := fmt.Sprintf("%s/api/v1/candles?market_id=%d&resolution=%s&start_timestamp=%d&end_timestamp=%d&count_back=%d",
		l.cfg.APIURL, marketInfo.MarketID, interval, startTime, endTime, limit)

	body, err := l.sendGetRequest(endpoint)
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Code    int `json:"code"`
		Candles []struct {
			T int64   `json:"t"`
			O float64 `json:"o"`
			H float64 `json:"h"`
			L float64 `json:"l"`
			C float64 `json:"c"`
			V float64 `json:"v"`
		} `json:"c"`
	}

	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	candles := make([]Candle, 0, len(apiResp.Candles))
	for _, c := range apiResp.Candles {
		candles = append(candles, Candle{
			OpenTime: c.T,
			Open:     c.O,
			High:     c.H,
			Low:      c.L,
			Close:    c.C,
			Volume:   c.V,
		})
	}

	return candles, nil
}

// GetPosition 获取持仓
func (l *LighterAdapter) GetPosition() (*Position, error) {
	acct, err := l.getDetailedAccount()
	if err != nil {
		return nil, err
	}

	normSymbol := normalizeSymbol(l.cfg.Symbol)
	for _, pos := range acct.Positions {
		if normalizeSymbol(pos.Symbol) == normSymbol {
			size, _ := strconv.ParseFloat(pos.Position, 64)
			entryPrice, _ := strconv.ParseFloat(pos.AvgEntryPrice, 64)
			unrealizedPnl, _ := strconv.ParseFloat(pos.UnrealizedPnl, 64)
			liqPrice, _ := strconv.ParseFloat(pos.LiquidationPrice, 64)

			imfVal, _ := strconv.ParseFloat(pos.InitialMarginFraction, 64)
			leverage := 1
			if imfVal > 0 {
				leverage = int(math.Round(100.0 / imfVal))
			}

			size = size * float64(pos.Sign)

			return &Position{
				Symbol:        pos.Symbol,
				Size:          size,
				EntryPrice:    entryPrice,
				UnrealizedPnl: unrealizedPnl,
				Leverage:      leverage,
				LiquidationPx: liqPrice,
			}, nil
		}
	}

	return &Position{
		Symbol: l.cfg.Symbol,
		Size:   0,
	}, nil
}

// GetBalance 获取账户可用余额（USDC）
func (l *LighterAdapter) GetBalance() (float64, error) {
	acct, err := l.getDetailedAccount()
	if err != nil {
		return 0, err
	}

	available, _ := strconv.ParseFloat(acct.AvailableBalance, 64)
	if available > 0 {
		return available, nil
	}

	for _, asset := range acct.Assets {
		if normalizeSymbol(asset.Symbol) == "USDC" {
			val, _ := strconv.ParseFloat(asset.Balance, 64)
			return val, nil
		}
	}

	return 0, nil
}

func (l *LighterAdapter) getServerTimeMilli() int64 {
	return time.Now().UnixMilli() + l.timeOffset
}

// CreateOrder 下单
func (l *LighterAdapter) CreateOrder(side OrderSide, orderType OrderTypeKind, quantity, price float64) (*OrderResponse, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return nil, err
	}

	baseAmount := int64(quantity * math.Pow10(marketInfo.SizeDecimals))

	var priceValue uint32
	var orderTypeValue uint8
	var timeInForce uint8
	var orderExpiry int64

	if orderType == OrderTypeLimit {
		orderTypeValue = 0 // LIMIT
		priceValue = uint32(price * math.Pow10(marketInfo.PriceDecimals))
		timeInForce = 1 // GoodTillTime
		orderExpiry = l.getServerTimeMilli() + 28*24*60*60*1000
	} else {
		orderTypeValue = 1 // MARKET
		timeInForce = 0    // IOC
		var protectedPrice float64
		
		exactPrice, ok := l.localOb.SimulateMarketOrder(side, quantity)
		if ok && exactPrice > 0 {
			// 如果本地订单簿足够，我们用算出的深度价格加减一个 tick 作为保护价
			tickSize := 1.0 / math.Pow10(marketInfo.PriceDecimals)
			if side == OrderSideBuy {
				protectedPrice = exactPrice + tickSize
			} else {
				protectedPrice = exactPrice - tickSize
			}
			utils.Logger.Info("使用本地订单簿精确计算市价单滑点", zap.Float64("exact_price", exactPrice), zap.Float64("protected_price", protectedPrice))
		} else {
			// 本地订单簿未就绪或流动性不足时，直接返回错误，由外层逻辑进行重试
			return nil, fmt.Errorf("本地订单簿未同步或流动性不足，无法安全计算市价单滑点，拒绝使用 5%% 回退")
		}
		priceValue = uint32(protectedPrice * math.Pow10(marketInfo.PriceDecimals))
		orderExpiry = 0 // ImmediateOrCancel / Market orders MUST have OrderExpiry = 0
	}

	var reduceOnlyValue uint8 = 0
	// 止盈单（SELL LIMIT 且不是市价平仓单）必须设置 ReduceOnly=1。
	// 根据 Lighter 官方文档，未设置 ReduceOnly 的反向限价单将被视为尝试建立反向空头仓位，
	// 从而扣除反向订单保证金（Order Margin）。当账户保证金不足时会被 L2 撮合引擎在周期结算时强制撤单。
	if side == OrderSideSell && orderType == OrderTypeLimit {
		reduceOnlyValue = 1
	}

	clientOrderIndex := time.Now().UnixNano() / 1000 % 281474976710655

	isAskValue := uint8(0)
	if side == OrderSideSell {
		isAskValue = 1
	}

	txReq := &types.CreateOrderTxReq{
		MarketIndex:      int16(marketInfo.MarketID),
		ClientOrderIndex: clientOrderIndex,
		BaseAmount:       baseAmount,
		Price:            priceValue,
		IsAsk:            isAskValue,
		Type:             orderTypeValue,
		TimeInForce:      timeInForce,
		ReduceOnly:       reduceOnlyValue,
		TriggerPrice:     0,
		OrderExpiry:      orderExpiry,
	}

	transactOpts := l.getTransactOpts()
	tx, err := l.txClient.GetCreateOrderTransaction(txReq, transactOpts)
	if err != nil {
		return nil, fmt.Errorf("sign order failed: %w", err)
	}

	txInfo, err := tx.GetTxInfo()
	if err != nil {
		return nil, fmt.Errorf("get tx info failed: %w", err)
	}

	txHash, err := l.submitOrder(int(tx.GetTxType()), txInfo)
	if err != nil {
		return nil, err
	}

	var orderID int64
	if orderType == OrderTypeLimit {
		// 使用 WebSocket Promise 模式等待订单确认，替换之前低效的 REST 轮询
		watchCh := l.wsManager.WatchOrder(clientOrderIndex)
		defer l.wsManager.UnwatchOrder(clientOrderIndex)

		utils.Logger.Debug("等待 WebSocket 订单确认...", zap.Int64("coi", clientOrderIndex))
		
		select {
		case wsOrder := <-watchCh:
			orderID = wsOrder.OrderIndex
			utils.Logger.Info("WebSocket 确认订单", zap.Int64("order_id", orderID))
		case <-time.After(10 * time.Second):
			utils.Logger.Warn("WebSocket 等待确认超时, 尝试回退到 REST 轮询", zap.Int64("coi", clientOrderIndex))
			orderID, err = l.pollForOrderIndex(clientOrderIndex)
			if err != nil {
				utils.Logger.Warn("REST 回退轮询失败", zap.Error(err))
				orderID = clientOrderIndex
			}
		}
	} else {
		orderID = clientOrderIndex
	}

	utils.Logger.Info("订单已提交",
		zap.String("side", string(side)),
		zap.String("type", string(orderType)),
		zap.Float64("price", price),
		zap.Float64("quantity", quantity),
		zap.Int64("order_id", orderID),
		zap.String("tx_hash", txHash))

	return &OrderResponse{
		OrderID: orderID,
		Status:  "resting",
		TxHash:  txHash,
	}, nil
}

// ModifyOrder 修改订单
func (l *LighterAdapter) ModifyOrder(orderID int64, side OrderSide, orderType OrderTypeKind, quantity, price float64) (*OrderResponse, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return nil, err
	}

	baseAmount := int64(quantity * math.Pow10(marketInfo.SizeDecimals))
	priceValue := uint32(price * math.Pow10(marketInfo.PriceDecimals))

	txReq := &types.ModifyOrderTxReq{
		MarketIndex:  int16(marketInfo.MarketID),
		Index:        orderID,
		BaseAmount:   baseAmount,
		Price:        priceValue,
		TriggerPrice: 0,
	}

	transactOpts := l.getTransactOpts()
	tx, err := l.txClient.GetModifyOrderTransaction(txReq, transactOpts)
	if err != nil {
		return nil, fmt.Errorf("sign modify order failed: %w", err)
	}

	txInfo, err := tx.GetTxInfo()
	if err != nil {
		return nil, fmt.Errorf("get tx info failed: %w", err)
	}

	txHash, err := l.submitOrder(int(tx.GetTxType()), txInfo)
	if err != nil {
		return nil, err
	}

	newOrderID, err := l.pollForModifiedOrderIndex(txHash)
	if err != nil {
		utils.Logger.Warn("poll modified order index failed, using old order ID as fallback", zap.Error(err))
		newOrderID = orderID
	}

	utils.Logger.Info("订单已修改",
		zap.Int64("orig_oid", orderID),
		zap.Int64("new_oid", newOrderID),
		zap.String("side", string(side)),
		zap.Float64("price", price),
		zap.Float64("quantity", quantity))

	return &OrderResponse{
		OrderID: newOrderID,
		Status:  "resting",
	}, nil
}

// CancelOrder 撤单
func (l *LighterAdapter) CancelOrder(orderID int64) error {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return err
	}

	txReq := &types.CancelOrderTxReq{
		MarketIndex: int16(marketInfo.MarketID),
		Index:       orderID,
	}

	transactOpts := l.getTransactOpts()
	tx, err := l.txClient.GetCancelOrderTransaction(txReq, transactOpts)
	if err != nil {
		return fmt.Errorf("sign cancel order failed: %w", err)
	}

	txInfo, err := tx.GetTxInfo()
	if err != nil {
		return fmt.Errorf("get tx info failed: %w", err)
	}

	_, err = l.submitOrder(int(tx.GetTxType()), txInfo)
	if err != nil {
		return err
	}

	utils.Logger.Info("订单已取消", zap.Int64("order_id", orderID))
	return nil
}

// CancelAllOrders 撤销全部订单
func (l *LighterAdapter) CancelAllOrders() error {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return err
	}

	marketIndex := int16(marketInfo.MarketID)
	txReq := &types.CancelAllOrdersTxReq{
		TimeInForce: 0,
	}

	transactOpts := l.getTransactOpts()
	transactOpts.TxAttributes.CancelAllMarketIndex = &marketIndex

	tx, err := l.txClient.GetCancelAllOrdersTransaction(txReq, transactOpts)
	if err != nil {
		return fmt.Errorf("sign cancel all orders failed: %w", err)
	}

	txInfo, err := tx.GetTxInfo()
	if err != nil {
		return fmt.Errorf("get tx info failed: %w", err)
	}

	_, err = l.submitOrder(int(tx.GetTxType()), txInfo)
	if err != nil {
		return err
	}

	utils.Logger.Info("批量取消订单完成", zap.Int16("market_id", marketIndex))
	return nil
}

// GetOpenOrders 获取未成交订单
func (l *LighterAdapter) GetOpenOrders() ([]OpenOrder, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return nil, err
	}

	orders, err := l.getActiveOrdersREST(marketInfo.MarketID)
	if err != nil {
		return nil, err
	}

	openOrders := make([]OpenOrder, 0, len(orders))
	for _, o := range orders {
		side := OrderSideBuy
		if o.IsAsk {
			side = OrderSideSell
		}

		orderType := OrderTypeLimit
		if o.Type == "market" {
			orderType = OrderTypeMarket
		}

		price, _ := strconv.ParseFloat(o.Price, 64)
		remAmt, _ := strconv.ParseFloat(o.RemainingBaseAmount, 64)
		if remAmt == 0 {
			remAmt, _ = strconv.ParseFloat(o.InitialBaseAmount, 64)
		}

		openOrders = append(openOrders, OpenOrder{
			OrderID:  o.OrderIndex,
			Side:     side,
			Type:     orderType,
			Price:    price,
			Quantity: remAmt,
			Symbol:   l.cfg.Symbol,
		})
	}

	return openOrders, nil
}

// GetSymbol 获取当前交易对名称
func (l *LighterAdapter) GetSymbol() string {
	return l.cfg.Symbol
}

// IsWSActive 返回 WS 连接是否活跃
func (l *LighterAdapter) IsWSActive() bool {
	return l.wsManager.IsWSActive()
}

// GetSymbolInfo 获取交易对精度信息
func (l *LighterAdapter) GetSymbolInfo() (*SymbolInfo, error) {
	l.symbolInfoMu.RLock()
	info := l.symbolInfo
	l.symbolInfoMu.RUnlock()

	if info != nil {
		return info, nil
	}

	if err := l.initSymbolInfo(); err != nil {
		return nil, err
	}

	l.symbolInfoMu.RLock()
	info = l.symbolInfo
	l.symbolInfoMu.RUnlock()

	return info, nil
}

// getMarketInfo gets the market configuration details for the symbol
func (l *LighterAdapter) getMarketInfo() (*MarketInfo, error) {
	endpoint := fmt.Sprintf("%s/api/v1/orderBooks", l.cfg.APIURL)
	body, err := l.sendGetRequest(endpoint)
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Code       int `json:"code"`
		OrderBooks []struct {
			Symbol                 string `json:"symbol"`
			MarketID               uint16 `json:"market_id"`
			Status                 string `json:"status"`
			SupportedSizeDecimals  int    `json:"supported_size_decimals"`
			SupportedPriceDecimals int    `json:"supported_price_decimals"`
		} `json:"order_books"`
	}

	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	normSymbol := normalizeSymbol(l.cfg.Symbol)
	for _, o := range apiResp.OrderBooks {
		if normalizeSymbol(o.Symbol) == normSymbol {
			return &MarketInfo{
				Symbol:        o.Symbol,
				MarketID:      o.MarketID,
				SizeDecimals:  o.SupportedSizeDecimals,
				PriceDecimals: o.SupportedPriceDecimals,
			}, nil
		}
	}

	fallbackID, err := getFallbackMarketIndex(normSymbol)
	if err == nil {
		return &MarketInfo{
			Symbol:        l.cfg.Symbol,
			MarketID:      fallbackID,
			SizeDecimals:  4,
			PriceDecimals: 4,
		}, nil
	}

	return nil, fmt.Errorf("unknown market symbol: %s", l.cfg.Symbol)
}

// initSymbolInfo initializes the local precision cached details
func (l *LighterAdapter) initSymbolInfo() error {
	mInfo, err := l.getMarketInfo()
	if err != nil {
		return err
	}

	info := &SymbolInfo{
		QuantityPrecision: mInfo.SizeDecimals,
		PricePrecision:    mInfo.PriceDecimals,
		MinQty:            math.Pow10(-mInfo.SizeDecimals),
		StepSize:          math.Pow10(-mInfo.SizeDecimals),
		TickSize:          math.Pow10(-mInfo.PriceDecimals),
		SzDecimals:        mInfo.SizeDecimals,
		MaxPriceDecimals:  mInfo.PriceDecimals,
	}

	l.symbolInfoMu.Lock()
	l.symbolInfo = info
	l.symbolInfoMu.Unlock()

	utils.Logger.Info("交易对信息初始化完成",
		zap.String("symbol", l.cfg.Symbol),
		zap.Int("price_precision", info.PricePrecision),
		zap.Int("qty_precision", info.QuantityPrecision),
		zap.Float64("step_size", info.StepSize),
		zap.Float64("tick_size", info.TickSize))

	return nil
}

// getDetailedAccount fetches the Lighter account data
func (l *LighterAdapter) getDetailedAccount() (*LighterDetailedAccount, error) {
	endpoint := fmt.Sprintf("%s/api/v1/account?by=index&value=%d", l.cfg.APIURL, l.cfg.AccountIndex)
	body, err := l.sendGetRequest(endpoint)
	if err != nil {
		return nil, err
	}

	var acctResp LighterAccountResponse
	if err := json.Unmarshal(body, &acctResp); err != nil {
		return nil, fmt.Errorf("解析账户 JSON 失败: %w", err)
	}

	if acctResp.Code != 200 {
		return nil, fmt.Errorf("Lighter API error: code %d, msg: %s", acctResp.Code, acctResp.Message)
	}

	if len(acctResp.Accounts) == 0 {
		return nil, fmt.Errorf("no accounts returned for index %d", l.cfg.AccountIndex)
	}

	return &acctResp.Accounts[0], nil
}

// nonceCounter 保证 SkipNonce 模式下 nonce 严格递增。
// 官方要求：2^47-1 > new_nonce > old_nonce。
// 使用毫秒时间戳（~1.75e12 < 2^47-1 ≈ 1.4e14）作为基数，
// 再通过原子 CAS 保证同进程内严格递增。
var nonceCounter atomic.Int64

// getTransactOpts generates TransactOpts using SkipNonce=1 and a monotonically
// increasing millisecond-timestamp nonce (must be < 2^47-1 per official docs).
func (l *LighterAdapter) getTransactOpts() *types.TransactOpts {
	var skipNonceVal uint8 = 1
	base := time.Now().UnixMilli()

	// 原子 CAS：确保 nonce 严格递增；若同一毫秒内多次调用，则在上一个值基础上 +1
	for {
		prev := nonceCounter.Load()
		next := base
		if next <= prev {
			next = prev + 1
		}
		if nonceCounter.CompareAndSwap(prev, next) {
			nonceVal := next
			return &types.TransactOpts{
				FromAccountIndex: &l.cfg.AccountIndex,
				ApiKeyIndex:      &l.cfg.ApiKeyIndex,
				Nonce:            &nonceVal,
				TxAttributes: &types.L2TxAttributes{
					SkipNonce: &skipNonceVal,
				},
			}
		}
	}
}

// getActiveOrdersREST fetches the active orders for the market
func (l *LighterAdapter) getActiveOrdersREST(marketID uint16) ([]wsLighterOrder, error) {
	token, err := l.getAuthToken()
	if err != nil {
		return nil, err
	}

	endpoint := fmt.Sprintf("%s/api/v1/accountActiveOrders?account_index=%d&market_id=%d",
		l.cfg.APIURL, l.cfg.AccountIndex, marketID)

	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("authorization", token)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var apiResp struct {
		Code   int              `json:"code"`
		Orders []wsLighterOrder `json:"orders"`
	}
	if err := json.Unmarshal(body, &apiResp); err != nil {
		return nil, err
	}

	return apiResp.Orders, nil
}

// pollForOrderIndex polls the active orders list to locate the order index assigned by matching engine
func (l *LighterAdapter) pollForOrderIndex(clientOrderIndex int64) (int64, error) {
	marketInfo, err := l.getMarketInfo()
	if err != nil {
		return 0, err
	}

	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		orders, err := l.getActiveOrdersREST(marketInfo.MarketID)
		if err != nil {
			continue
		}
		for _, o := range orders {
			if o.ClientOrderIndex == clientOrderIndex {
				return o.OrderIndex, nil
			}
		}
	}
	return 0, fmt.Errorf("order index not found for clientOrderIndex %d", clientOrderIndex)
}

// pollForModifiedOrderIndex queries transaction detail by hash and parses event_info to get the new order index
func (l *LighterAdapter) pollForModifiedOrderIndex(txHash string) (int64, error) {
	endpoint := fmt.Sprintf("%s/api/v1/tx?by=hash&value=%s", l.cfg.APIURL, txHash)

	for i := 0; i < 30; i++ {
		time.Sleep(100 * time.Millisecond)
		body, err := l.sendGetRequest(endpoint)
		if err != nil {
			continue
		}

		var txResp struct {
			Code      int    `json:"code"`
			EventInfo string `json:"event_info"`
			Status    int    `json:"status"`
		}
		if err := json.Unmarshal(body, &txResp); err != nil {
			continue
		}

		if txResp.Code == 200 && txResp.EventInfo != "" {
			var ev struct {
				NewOrder struct {
					Index int64 `json:"i"`
				} `json:"no"`
			}
			if err := json.Unmarshal([]byte(txResp.EventInfo), &ev); err == nil && ev.NewOrder.Index > 0 {
				return ev.NewOrder.Index, nil
			}
		}
	}
	return 0, fmt.Errorf("failed to get modified order index from tx %s", txHash)
}

// getAuthToken generates Lighter Auth Token
func (l *LighterAdapter) getAuthToken() (string, error) {
	token, err := l.txClient.GetAuthToken(time.Now().Add(7 * time.Hour))
	if err != nil {
		return "", err
	}
	return token, nil
}

// restPriceFallback REST 降级轮询最新成交价
func (l *LighterAdapter) restPriceFallback() {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("restPriceFallback panic 恢复", zap.Any("recover", r))
		}
	}()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-l.ctx.Done():
			return
		case <-ticker.C:
			if l.wsManager.IsWSActive() {
				continue
			}

			price, err := l.GetLatestPrice()
			if err != nil {
				utils.Logger.Error("REST 轮询价格失败", zap.Error(err))
				continue
			}

			l.bus.Publish(core.EventTick, &PriceUpdate{
				Price:     price,
				Timestamp: time.Now().UnixMilli(),
			})
			utils.Logger.Debug("REST 降级推送价格", zap.Float64("price", price))
		}
	}
}

// submitOrder Submit signed order to LIGHTER API using multipart/form-data
func (l *LighterAdapter) submitOrder(txType int, txInfo string) (string, error) {
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	if err := writer.WriteField("tx_type", strconv.Itoa(txType)); err != nil {
		return "", fmt.Errorf("failed to write tx_type: %w", err)
	}

	if err := writer.WriteField("tx_info", txInfo); err != nil {
		return "", fmt.Errorf("failed to write tx_info: %w", err)
	}

	if err := writer.WriteField("price_protection", "false"); err != nil {
		return "", fmt.Errorf("failed to write price_protection: %w", err)
	}

	if err := writer.Close(); err != nil {
		return "", fmt.Errorf("failed to close multipart writer: %w", err)
	}

	endpoint := fmt.Sprintf("%s/api/v1/sendTx", l.cfg.APIURL)
	httpReq, err := http.NewRequest("POST", endpoint, &body)
	if err != nil {
		return "", err
	}

	httpReq.Header.Set("Content-Type", writer.FormDataContentType())
	httpReq.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	// ★ 官方文档要求：对每个请求进行认证以绕过 IP 速率限制。
	// 下单交易必须携带授权头，否则 AWS WAF 会对机房 IP 触发人机验证（HTTP 405）。
	token, err := l.getAuthToken()
	if err != nil {
		return "", fmt.Errorf("生成认证 Token 失败: %w", err)
	}
	httpReq.Header.Set("authorization", token)

	resp, err := l.httpClient.Do(httpReq)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	var sendResp struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		TxHash  string `json:"tx_hash"`
	}
	if err := json.Unmarshal(respBody, &sendResp); err != nil {
		return "", fmt.Errorf("failed to parse response: %w, body: %s", err, string(respBody))
	}

	if sendResp.Code != 200 {
		return "", fmt.Errorf("failed to submit order (code %d): %s", sendResp.Code, sendResp.Message)
	}

	return sendResp.TxHash, nil
}

// MarketInfo 封装交易对关键信息
type MarketInfo struct {
	Symbol        string
	MarketID      uint16
	SizeDecimals  int
	PriceDecimals int
}

// getFallbackMarketIndex 提供硬编码备用 ID
func getFallbackMarketIndex(symbol string) (uint16, error) {
	if symbol == "HYPE" {
		return 4, nil
	}
	return 0, fmt.Errorf("fallback market not found for %s (HYPE is the only supported market)", symbol)
}

// normalizeSymbol 标准化符号为 Lighter Perp 命名习惯
func normalizeSymbol(symbol string) string {
	s := strings.TrimSuffix(symbol, "-PERP")
	s = strings.TrimSuffix(s, "USDT")
	s = strings.TrimSuffix(s, "USDC")
	s = strings.TrimSuffix(s, "/USDT")
	s = strings.TrimSuffix(s, "/USDC")
	return strings.ToUpper(s)
}

// intervalToDuration 解析 K 线时间周期为 time.Duration
func intervalToDuration(interval string) time.Duration {
	switch interval {
	case "1m":
		return time.Minute
	case "5m":
		return 5 * time.Minute
	case "15m":
		return 15 * time.Minute
	case "30m":
		return 30 * time.Minute
	case "1h":
		return time.Hour
	case "4h":
		return 4 * time.Hour
	case "12h":
		return 12 * time.Hour
	case "1d":
		return 24 * time.Hour
	default:
		return time.Hour
	}
}


