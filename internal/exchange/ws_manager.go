package exchange

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/uykb/MartinStrategy/internal/core"
	"github.com/uykb/MartinStrategy/internal/utils"
	"go.uber.org/zap"
)

// wsLighterRequest 表示发往 Lighter WebSocket 的请求
type wsLighterRequest struct {
	Type    string `json:"type"`              // "subscribe" | "unsubscribe" | "ping"
	Channel string `json:"channel,omitempty"` // 订阅通道名称
	Auth    string `json:"auth,omitempty"`    // 鉴权 token
}

// wsLighterEnvelope 表示 Lighter WebSocket 推送消息的统一信封
type wsLighterEnvelope struct {
	Type      string                      `json:"type"`
	Channel   string                      `json:"channel"`
	Timestamp int64                       `json:"timestamp"`
	Ticker    *wsLighterTicker            `json:"ticker,omitempty"`
	Orders    map[string][]wsLighterOrder `json:"orders,omitempty"`
}

type wsLighterTicker struct {
	S string `json:"s"`
	A struct {
		Price string `json:"price"`
		Size  string `json:"size"`
	} `json:"a"`
	B struct {
		Price string `json:"price"`
		Size  string `json:"size"`
	} `json:"b"`
}

type wsLighterOrder struct {
	OrderIndex          int64  `json:"order_index"`
	ClientOrderIndex    int64  `json:"client_order_index"`
	OrderID             string `json:"order_id"`
	ClientOrderID       string `json:"client_order_id"`
	MarketIndex         int16  `json:"market_index"`
	OwnerAccountIndex   int64  `json:"owner_account_index"`
	InitialBaseAmount   string `json:"initial_base_amount"`
	Price               string `json:"price"`
	Nonce               int64  `json:"nonce"`
	RemainingBaseAmount string `json:"remaining_base_amount"`
	IsAsk               bool   `json:"is_ask"`
	Side                string `json:"side"`
	Type                string `json:"type"`
	ReduceOnly          bool   `json:"reduce_only"`
	TriggerPrice        string `json:"trigger_price"`
	OrderExpiry         int64  `json:"order_expiry"`
	Status              string `json:"status"`
}

const (
	wsPingInterval        = 30 * time.Second
	wsPongTimeout         = 10 * time.Second
	wsMaxReconnectRetries = 10
	wsInitialBackoff      = 2 * time.Second
	wsMaxBackoff          = 60 * time.Second
	wsWriteTimeout        = 5 * time.Second
	wsReadTimeout         = 2*time.Minute + 10*time.Second // Lighter requires at least one frame every 2 minutes
)

// WSManager 管理 Lighter WebSocket 连接
type WSManager struct {
	cfg     *LighterConfig
	bus     *core.EventBus
	adapter *LighterAdapter

	connMu sync.Mutex
	conn   *websocket.Conn

	ctx    context.Context
	cancel context.CancelFunc

	pongCh       chan struct{}
	reconnectMu  sync.Mutex
	reconnecting bool

	wsActive atomic.Bool

	priceEventCh chan *PriceUpdate
	orderEventCh chan *OrderUpdate

	wg sync.WaitGroup
}

// NewWSManager 创建 WebSocket 管理器
func NewWSManager(cfg *LighterConfig, bus *core.EventBus, adapter *LighterAdapter) *WSManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &WSManager{
		cfg:          cfg,
		bus:          bus,
		adapter:      adapter,
		ctx:          ctx,
		cancel:       cancel,
		pongCh:       make(chan struct{}, 1),
		priceEventCh: make(chan *PriceUpdate, 500),
		orderEventCh: make(chan *OrderUpdate, 200),
	}
}

// IsWSActive 返回 WS 连接是否活跃
func (w *WSManager) IsWSActive() bool {
	return w.wsActive.Load()
}

// Start 启动 WebSocket 管理器
func (w *WSManager) Start() error {
	if err := w.connect(); err != nil {
		return fmt.Errorf("WSManager.Start: 连接失败: %w", err)
	}

	if err := w.sendSubscriptions(); err != nil {
		return fmt.Errorf("WSManager.Start: 订阅失败: %w", err)
	}

	w.wg.Add(1)
	go w.readLoop()

	w.wg.Add(1)
	go w.heartbeatLoop()

	w.wg.Add(1)
	go w.dispatchPriceEvents()

	w.wg.Add(1)
	go w.dispatchOrderEvents()

	utils.Logger.Info("WSManager 启动成功",
		zap.String("ws_url", w.cfg.WSURL),
		zap.String("symbol", w.cfg.Symbol))

	return nil
}

// Stop 关闭 WebSocket 管理器
func (w *WSManager) Stop() {
	utils.Logger.Info("WSManager 正在关闭...")
	w.wsActive.Store(false)
	w.cancel()

	w.connMu.Lock()
	if w.conn != nil {
		w.conn.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
			time.Now().Add(time.Second))
		w.conn.Close()
		w.conn = nil
	}
	w.connMu.Unlock()

	w.wg.Wait()
	utils.Logger.Info("WSManager 已关闭")
}

func (w *WSManager) connect() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, resp, err := dialer.Dial(w.cfg.WSURL, nil)
	if err != nil {
		if resp != nil {
			bodyBytes, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("WebSocket 拨号失败 (HTTP 状态码: %d, 响应: %s): %w", resp.StatusCode, string(bodyBytes), err)
		}
		return fmt.Errorf("WebSocket 拨号失败: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
	w.conn = conn
	w.wsActive.Store(true)

	utils.Logger.Info("WebSocket 连接建立成功", zap.String("url", w.cfg.WSURL))
	return nil
}

func (w *WSManager) sendSubscriptions() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn == nil {
		return fmt.Errorf("WebSocket 未连接")
	}

	marketInfo, err := w.adapter.getMarketInfo()
	if err != nil {
		return fmt.Errorf("获取交易对信息失败: %w", err)
	}

	// 1. 订阅 Ticker 公共流
	tickerChannel := fmt.Sprintf("ticker/%d", marketInfo.MarketID)
	subTicker := wsLighterRequest{
		Type:    "subscribe",
		Channel: tickerChannel,
	}

	tickerData, err := json.Marshal(subTicker)
	if err != nil {
		return err
	}

	if err := w.conn.WriteMessage(websocket.TextMessage, tickerData); err != nil {
		return fmt.Errorf("发送 Ticker 订阅失败: %w", err)
	}
	utils.Logger.Info("已发送 Ticker 订阅请求", zap.String("channel", tickerChannel))

	// 2. 订阅账户订单私有流
	token, err := w.adapter.getAuthToken()
	if err != nil {
		return fmt.Errorf("生成 Auth Token 失败: %w", err)
	}

	orderChannel := fmt.Sprintf("account_orders/%d/%d", marketInfo.MarketID, w.cfg.AccountIndex)
	subOrder := wsLighterRequest{
		Type:    "subscribe",
		Channel: orderChannel,
		Auth:    token,
	}

	orderData, err := json.Marshal(subOrder)
	if err != nil {
		return err
	}

	if err := w.conn.WriteMessage(websocket.TextMessage, orderData); err != nil {
		return fmt.Errorf("发送账户订单订阅失败: %w", err)
	}
	utils.Logger.Info("已发送账户订单订阅请求", zap.String("channel", orderChannel))

	return nil
}

func (w *WSManager) heartbeatLoop() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("heartbeatLoop panic 恢复", zap.Any("recover", r))
		}
	}()

	ticker := time.NewTicker(wsPingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return
		case <-ticker.C:
			if err := w.sendPing(); err != nil {
				utils.Logger.Warn("发送 ping 失败，触发重连", zap.Error(err))
				go w.triggerReconnect()
				return
			}

			select {
			case <-w.pongCh:
				utils.Logger.Debug("收到 pong，连接正常")
			case <-time.After(wsPongTimeout):
				utils.Logger.Warn("pong 超时，判定连接假死，触发重连")
				go w.triggerReconnect()
				return
			}
		}
	}
}

func (w *WSManager) sendPing() error {
	w.connMu.Lock()
	defer w.connMu.Unlock()

	if w.conn == nil {
		return fmt.Errorf("连接不存在")
	}

	ping := wsLighterRequest{Type: "ping"}
	data, err := json.Marshal(ping)
	if err != nil {
		return err
	}

	if err := w.conn.WriteMessage(websocket.TextMessage, data); err != nil {
		return err
	}
	utils.Logger.Debug("已发送 ping")
	return nil
}

func (w *WSManager) triggerReconnect() {
	w.reconnectMu.Lock()
	if w.reconnecting {
		w.reconnectMu.Unlock()
		return
	}
	w.reconnecting = true
	w.reconnectMu.Unlock()

	defer func() {
		w.reconnectMu.Lock()
		w.reconnecting = false
		w.reconnectMu.Unlock()
	}()

	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("triggerReconnect panic 恢复", zap.Any("recover", r))
		}
	}()

	w.reconnectWithBackoff()
}

func (w *WSManager) reconnectWithBackoff() {
	backoff := wsInitialBackoff

	for attempt := 1; attempt <= wsMaxReconnectRetries; attempt++ {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		utils.Logger.Info("尝试重连 WebSocket...",
			zap.Int("attempt", attempt),
			zap.Duration("backoff", backoff))

		select {
		case <-w.ctx.Done():
			return
		case <-time.After(backoff):
		}

		w.connMu.Lock()
		if w.conn != nil {
			w.conn.Close()
			w.conn = nil
		}
		w.connMu.Unlock()

		w.wsActive.Store(false)

		if err := w.connect(); err != nil {
			utils.Logger.Error("重连失败", zap.Int("attempt", attempt), zap.Error(err))
			backoff *= 2
			if backoff > wsMaxBackoff {
				backoff = wsMaxBackoff
			}
			continue
		}

		if err := w.sendSubscriptions(); err != nil {
			utils.Logger.Error("重新订阅失败", zap.Error(err))
			continue
		}

		utils.Logger.Info("WebSocket 重连成功", zap.Int("attempt", attempt))
		w.resyncViaREST()

		w.wg.Add(1)
		go w.readLoop()

		w.wg.Add(1)
		go w.heartbeatLoop()

		return
	}

	utils.Logger.Error("WebSocket 重连失败：已达最大重试次数", zap.Int("max_retries", wsMaxReconnectRetries))
}

func (w *WSManager) resyncViaREST() {
	utils.Logger.Info("开始 REST 对账 (Resync)...")
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("resyncViaREST panic 恢复", zap.Any("recover", r))
		}
	}()

	w.bus.Publish(core.EventResyncStart, nil)

	unfrozen := false
	defer func() {
		if !unfrozen {
			w.bus.Publish(core.EventResyncEnd, nil)
			utils.Logger.Info("REST 对账完成（defer 解冻）")
		}
	}()

	pos, err := w.adapter.GetPosition()
	if err != nil {
		utils.Logger.Error("REST 对账：查询持仓失败", zap.Error(err))
		return
	}

	if pos == nil || pos.Size == 0 {
		pos = &Position{Symbol: w.cfg.Symbol, Size: 0}
	}

	w.bus.Publish(core.EventResyncEnd, nil)
	unfrozen = true
	utils.Logger.Info("REST 对账完成，已解冻 FSM，开始发布校准事件")

	w.bus.Publish(core.EventPositionUpdate, pos)
}

func (w *WSManager) readLoop() {
	defer w.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("readLoop panic 恢复", zap.Any("recover", r))
			go w.triggerReconnect()
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		default:
		}

		w.connMu.Lock()
		conn := w.conn
		w.connMu.Unlock()

		if conn == nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		conn.SetReadDeadline(time.Now().Add(wsReadTimeout))

		_, message, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				utils.Logger.Info("WebSocket 正常关闭")
				return
			}

			utils.Logger.Warn("WebSocket 读取错误，触发重连", zap.Error(err))
			go w.triggerReconnect()
			return
		}

		w.handleMessage(message)
	}
}

func (w *WSManager) handleMessage(message []byte) {
	defer func() {
		if r := recover(); r != nil {
			utils.Logger.Error("handleMessage panic 恢复",
				zap.Any("recover", r),
				zap.String("message", string(message)))
		}
	}()

	var env wsLighterEnvelope
	if err := json.Unmarshal(message, &env); err != nil {
		// Try to parse ping/pong direct responses
		var simple struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(message, &simple) == nil && simple.Type == "pong" {
			select {
			case w.pongCh <- struct{}{}:
			default:
			}
		}
		return
	}

	if env.Type == "pong" {
		select {
		case w.pongCh <- struct{}{}:
		default:
		}
		return
	}

	switch {
	case strings.HasPrefix(env.Type, "update/ticker"):
		w.handleTicker(env.Ticker)
	case strings.HasPrefix(env.Type, "update/account_orders") || strings.HasPrefix(env.Type, "subscribed/account_orders"):
		w.handleAccountOrders(env.Orders)
	}
}

func (w *WSManager) handleTicker(t *wsLighterTicker) {
	if t == nil {
		return
	}

	priceStr := t.B.Price
	if priceStr == "" {
		priceStr = t.A.Price
	}
	if priceStr == "" {
		return
	}

	price, err := strconv.ParseFloat(priceStr, 64)
	if err != nil {
		utils.Logger.Error("解析 WebSocket 价格失败", zap.String("val", priceStr), zap.Error(err))
		return
	}

	update := &PriceUpdate{
		Price:     price,
		Timestamp: time.Now().UnixMilli(),
	}

	select {
	case w.priceEventCh <- update:
	default:
	}
}

func (w *WSManager) handleAccountOrders(orders map[string][]wsLighterOrder) {
	if orders == nil {
		return
	}

	for _, list := range orders {
		for _, o := range list {
			side := OrderSideBuy
			if o.IsAsk {
				side = OrderSideSell
			}

			orderType := OrderTypeLimit
			if o.Type == "market" {
				orderType = OrderTypeMarket
			}

			status := strings.ToUpper(o.Status)
			if strings.HasPrefix(o.Status, "canceled") {
				status = "CANCELED"
			} else if o.Status == "open" {
				status = "NEW"
			}

			execPrice, _ := strconv.ParseFloat(o.Price, 64)
			qty, _ := strconv.ParseFloat(o.RemainingBaseAmount, 64)

			update := &OrderUpdate{
				OrderID:   o.OrderIndex,
				Symbol:    w.cfg.Symbol,
				Side:      side,
				Type:      orderType,
				Status:    status,
				ExecPrice: execPrice,
				Quantity:  qty,
			}

			select {
			case w.orderEventCh <- update:
			default:
			}
		}
	}
}

func (w *WSManager) dispatchPriceEvents() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case update := <-w.priceEventCh:
			w.bus.Publish(core.EventTick, update)
		}
	}
}

func (w *WSManager) dispatchOrderEvents() {
	defer w.wg.Done()
	for {
		select {
		case <-w.ctx.Done():
			return
		case update := <-w.orderEventCh:
			w.bus.Publish(core.EventOrderUpdate, update)
		}
	}
}
