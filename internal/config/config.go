// Package config 提供交易所/策略/日志/健康检查的类型定义。
//
// 配置来源：
//   - 配置由 cmd/bot/main.go 通过 kong 解析命令行参数或环境变量（前缀 MARTIN_）构造
//   - 不再支持 YAML/config.yaml（Docker 部署统一使用环境变量），已移除 viper 依赖
package config

// Config 根配置结构
type Config struct {
	Exchange ExchangeConfig
	Strategy StrategyConfig
	Log      LogConfig
	Health   *HealthConfig // ★ P2 加固：健康检查配置
}

// ExchangeConfig 交易所配置
//
// Lighter 适配说明：
//   - ApiKey:       Lighter API Key 的私钥（Hex 格式）
//   - Account:      Lighter Account Index 或 L1 钱包地址（Hex 格式，含 0x 前缀）
//   - ApiKeyIndex:  Lighter API Key Index (通常为 2-254)
//   - Symbol:       交易对名称（固定为 "HYPE"）
//   - UseTestnet:   是否使用 Lighter 测试网
type ExchangeConfig struct {
	ApiKey      string // Lighter API key private key
	Account     string // Lighter Account Index 或 L1 Wallet Address
	ApiKeyIndex uint8  // Lighter API Key Index (通常为 2-254)
	Symbol      string // 交易对（固定为 "HYPE"）
	UseTestnet  bool   // 是否使用测试网
}

// StrategyConfig 策略配置
type StrategyConfig struct {
	MaxSafetyOrders int // 最大网格层数
}

// LogConfig 日志配置
type LogConfig struct {
	Level string
}

// HealthConfig 健康检查配置（★ P2 加固）
type HealthConfig struct {
	Addr string // 监听地址（如 ":8080"）
}
