// Package utils 提供通用工具函数。
// price_rounder.go 实现了价格截断与格式化规则，防止下单被拒。
package utils

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ---------------------------------------------------------------------------
// 价格精度规则
// ---------------------------------------------------------------------------

// RoundToSigFigs 将价格截断到指定有效数字位数，并限制最大小数位数。
//
// 参数：
//   - price:      原始价格
//   - sigFigs:    有效数字位数
//   - maxDecimals: 最大允许小数位数
//
// 返回：
//   - 截断后的价格（float64）
//
// 示例：
//
//	RoundToSigFigs(102.3456, 5, 2) → 102.35
//	RoundToSigFigs(0.00123456, 5, 6) → 0.0012346
//	RoundToSigFigs(100000.0, 5, 0) → 100000（整数价格始终合法）
func RoundToSigFigs(price float64, sigFigs int, maxDecimals int) float64 {
	// 防御性检查
	if price == 0 || sigFigs <= 0 {
		return 0
	}

	// 规则 3：整数价格始终合法
	if price == math.Trunc(price) && math.Abs(price) >= 1 {
		return price
	}

	// 规则 1：截断到 sigFigs 位有效数字
	rounded := roundToSignificantFigures(price, sigFigs)

	// 规则 2：限制最大小数位数
	if maxDecimals >= 0 {
		pow := math.Pow(10, float64(maxDecimals))
		rounded = math.Round(rounded*pow) / pow
	}

	return rounded
}

// roundToSignificantFigures 将浮点数截断到 n 位有效数字
// 使用 Go 标准库的格式化能力避免浮点精度陷阱
func roundToSignificantFigures(val float64, n int) float64 {
	if val == 0 || n <= 0 {
		return 0
	}

	// 利用 Go 的 %g 格式化：自动选择 %e 或 %f 中更紧凑的表示
	// %g 的精度参数即为有效数字位数
	format := fmt.Sprintf("%%.%dg", n)
	str := fmt.Sprintf(format, val)

	// 解析回 float64
	result, err := parseFloat(str)
	if err != nil {
		// 降级：直接返回原值
		return val
	}
	return result
}

// parseFloat 安全解析浮点数字符串，兼容科学计数法
func parseFloat(s string) (float64, error) {
	s = strings.TrimSpace(s)
	return strconv.ParseFloat(s, 64)
}
