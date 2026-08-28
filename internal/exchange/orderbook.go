package exchange

import (
	"sort"
	"sync"
	"time"
)

type OrderbookLevel struct {
	Price float64
	Size  float64
}

type Orderbook struct {
	mu          sync.RWMutex
	bids        map[float64]float64
	asks        map[float64]float64
	lastUpdated int64
}

func NewOrderbook() *Orderbook {
	return &Orderbook{
		bids: make(map[float64]float64),
		asks: make(map[float64]float64),
	}
}

func (ob *Orderbook) ApplySnapshot(bids, asks []OrderbookLevel) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.bids = make(map[float64]float64)
	ob.asks = make(map[float64]float64)
	for _, b := range bids {
		if b.Size > 0 {
			ob.bids[b.Price] = b.Size
		}
	}
	for _, a := range asks {
		if a.Size > 0 {
			ob.asks[a.Price] = a.Size
		}
	}
	ob.lastUpdated = time.Now().UnixMilli()
}

func (ob *Orderbook) ApplyDelta(bids, asks []OrderbookLevel) {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	for _, b := range bids {
		if b.Size <= 0 {
			delete(ob.bids, b.Price)
		} else {
			ob.bids[b.Price] = b.Size
		}
	}
	for _, a := range asks {
		if a.Size <= 0 {
			delete(ob.asks, a.Price)
		} else {
			ob.asks[a.Price] = a.Size
		}
	}
	ob.lastUpdated = time.Now().UnixMilli()
}

func (ob *Orderbook) Clear() {
	ob.mu.Lock()
	defer ob.mu.Unlock()
	ob.bids = make(map[float64]float64)
	ob.asks = make(map[float64]float64)
}

// SimulateMarketOrder 计算市价单吃单后的边缘价格
// side: "BUY" 或 "SELL"
// 返回值: (到达的限价深度, 是否足够流动性)
func (ob *Orderbook) SimulateMarketOrder(side OrderSide, qty float64) (float64, bool) {
	ob.mu.RLock()
	defer ob.mu.RUnlock()

	rem := qty
	var worstPrice float64

	if side == OrderSideBuy {
		// Buy 吃 Ask (价格从小到大)
		var prices []float64
		for p := range ob.asks {
			prices = append(prices, p)
		}
		sort.Float64s(prices)

		for _, p := range prices {
			sz := ob.asks[p]
			worstPrice = p
			if rem <= sz {
				rem = 0
				break
			}
			rem -= sz
		}
	} else {
		// Sell 吃 Bid (价格从大到小)
		var prices []float64
		for p := range ob.bids {
			prices = append(prices, p)
		}
		sort.Slice(prices, func(i, j int) bool { return prices[i] > prices[j] })

		for _, p := range prices {
			sz := ob.bids[p]
			worstPrice = p
			if rem <= sz {
				rem = 0
				break
			}
			rem -= sz
		}
	}

	if rem > 1e-8 {
		// 流动性不足
		return worstPrice, false
	}
	return worstPrice, true
}
