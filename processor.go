package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"

	"github.com/conduitio/conduit-commons/config"
	"github.com/conduitio/conduit-commons/opencdc"
	sdk "github.com/conduitio/conduit-processor-sdk"
	"github.com/rs/zerolog"
)

//go:generate paramgen -output=paramgen_proc.go ProcessorConfig

// limiting to llama3.2 for MVP
var allowedModels = []string{
	"llama3.2",
}

type Processor struct {
	sdk.UnimplementedProcessor
	referenceResolver sdk.ReferenceResolver

	config ProcessorConfig
}

type ProcessorConfig struct {
	// OllamaURL is the url to the ollama instance
	OllamaURL string `json:"url" validate:"required"`
	// Model is the name of the model used with ollama
	Model string `json:"model" default:"llama3.2"`
	// Prompt is the prompt to pass into ollama to tranform the data
	Prompt string `json:"prompt" default:""`
}

func NewProcessor() sdk.Processor {
	// Create Processor and wrap it in the default middleware.
	return sdk.ProcessorWithMiddleware(&Processor{}, sdk.DefaultProcessorMiddleware()...)
}

func (p *Processor) Configure(ctx context.Context, cfg config.Config) error {
	err := sdk.ParseConfig(ctx, cfg, &p.config, ProcessorConfig{}.Parameters())
	if err != nil {
		return fmt.Errorf("failed to parse configuration: %w", err)
	}

	// TODO dont need?
	// resolver, err := sdk.NewReferenceResolver(p.config.Field)
	// if err != nil {
	// 	return fmt.Errorf("failed to parse the %q param: %w", "field", err)
	// }
	// p.referenceResolver = resolver
	return nil
}

func (p *Processor) Specification() (sdk.Specification, error) {
	return sdk.Specification{
		Name:        "conduit-processor-ollama",
		Summary:     "Processes data through an ollama instance",
		Description: "This processor transforms data by asking the provided model on the provided ollama instance.",
		Version:     "devel",
		Author:      "Sarah Sicard",
		Parameters:  p.config.Parameters(),
	}, nil
}

func (p *Processor) Process(ctx context.Context, records []opencdc.Record) []sdk.ProcessedRecord {
	// TODO error: should return the slice at that index length.
	logger := sdk.Logger(ctx)
	logger.Info().Msg("Processing ollama records")

	if !slices.Contains(allowedModels, p.config.Model) {
		logger.Error().Msg("Model not allowed")
	}

	// create template for ollama call
	baseURL := fmt.Sprintf("curl %s/api/generate", p.config.OllamaURL)

	// need to loop through record by record in order to put an errors in the appropriate index for record
	result := make([]sdk.ProcessedRecord, len(records))
	for i, rec := range records {
		prompt := generatePrompt(p.config.Prompt, rec, logger)

		data := map[string]string{
			"model":  p.config.Model,
			"prompt": prompt,
		}
		jsonData, err := json.Marshal(data)
		if err != nil {
			logger.Error().Err(err).Msg("error marshalling json")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("error marshalling json")}
			continue
		}

		// is -d here?
		req, err := http.NewRequest("POST", baseURL, bytes.NewBuffer(jsonData))
		if err != nil {
			logger.Error().Err(err).Msg("unable to create request")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("unable to create request")}
			continue
		}

		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			logger.Error().Err(err).Msg("sending the request failed")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("sending the request failed")}
			continue
		}
		defer resp.Body.Close()

		body, err := io.ReadAll(resp.Body)
		if err != nil {
			logger.Error().Err(err).Msg("reading body of call")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("reading body of call")}
			continue
		}

		logger.Info().Msg(fmt.Sprintf("Response from ollama call: %s", string(body)))
		// assume its json
		respJson, err := json.Marshal(string(body))
		if err != nil {
			logger.Error().Err(err).Msg("unable to marshal json")
			result[i] = sdk.ErrorRecord{Error: fmt.Errorf("error marshalling json")}
			continue
		}
		// convert returned json into opencdrc.StructuredData? or RawData?
		rec.Payload.After = opencdc.RawData(respJson)

		result[i] = sdk.SingleRecord(rec)

	}

	return result
}

func generatePrompt(userPrompt string, record opencdc.Record, logger *zerolog.Logger) string {
	// TODO securing against malicious prompts
	// TODO limit size of the input
	conduitPrefix := "For the following records, return a json list of records following the instructions provided. Only send back records in the json format with no explanation."

	prompt := fmt.Sprintf(
		"%s \n Instructions: {%s}\n Record: {%s}",
		conduitPrefix,
		userPrompt,
		record)
	logger.Info().Msg(fmt.Sprintf("Sending message to ollama with the following prompt: %s", prompt))

	return prompt
}
