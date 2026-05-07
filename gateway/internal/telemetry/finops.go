package telemetry

// EstimateCostSaved calculates the saved USD amount based on token counts.
// Prices are estimated per 1M input tokens as of early 2024.
func EstimateCostSaved(provider, model string, tokensSaved int) float64 {
	if tokensSaved <= 0 {
		return 0
	}

	pricePerMillion := 0.0
	switch model {
	case "claude-3-5-sonnet", "claude-3-sonnet":
		pricePerMillion = 3.0
	case "claude-3-opus":
		pricePerMillion = 15.0
	case "claude-3-haiku":
		pricePerMillion = 0.25
	case "gpt-4o", "gpt-4-turbo":
		pricePerMillion = 5.0
	case "gpt-4":
		pricePerMillion = 30.0
	case "gpt-3.5-turbo":
		pricePerMillion = 0.5
	default:
		// Fallback for unknown models (e.g. Bedrock variants)
		pricePerMillion = 3.0
	}

	return (float64(tokensSaved) / 1000000.0) * pricePerMillion
}
