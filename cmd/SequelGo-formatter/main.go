package main

import (
	"SequelGo/internal/formatter"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "SequelGo-format [flags] <sql to parse>",
	Short: "Format T-SQL code",
	Args:  cobra.ExactArgs(1),
	Long: `SequelGo-format is an opionated formatter that formats T-SQL
code into a more readable format`,
	PreRunE: validStringEnums,
	Run:     runFormatter,
}
var (
	indentCommaListsStr     string
	indentCommaLists        formatter.IndentCommaLists
	indentInLists           bool
	indentBetweenConditions bool
	keywordCaseStr          string
	keywordCase             formatter.KeywordCase
	maxWidth                uint32
	indentWidth             uint32
	useTab                  bool
)

func validStringEnums(cmd *cobra.Command, args []string) error {
	if indentCommaListsStr == "NoSpaceAfterComma" {
		indentCommaLists = formatter.ICLNoSpaceAfterComma
	} else if indentCommaListsStr == "SpaceAfterComma" {
		indentCommaLists = formatter.ICLSpaceAfterComma
	} else if indentCommaListsStr == "TrailingComma" {
		indentCommaLists = formatter.ICLTrailingComma
	} else {
		msg := "only 'SpaceAfterComma', 'TrailingComma', or 'NoSpaceAfterComma"
		msg += " for IndentCommaLists"
		return fmt.Errorf(msg)
	}

	if keywordCaseStr == "UpperCase" {
		keywordCase = formatter.KCUpper
	} else if keywordCaseStr == "LowerCase" {
		keywordCase = formatter.KCLower
	} else {
		msg := "only 'UpperCase' or 'LowerCase"
		msg += " for KeywordCase"
		return fmt.Errorf(msg)
	}

	return nil
}

func runFormatter(cmd *cobra.Command, args []string) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	sugar := logger.Sugar()

	str := args[0]
	settings := formatter.Settings{
		IndentCommaLists:        indentCommaLists,
		IndentInLists:           indentInLists,
		IndentBetweenConditions: indentBetweenConditions,
		KeywordCase:             keywordCase,
		MaxWidth:                maxWidth,
		IndentWidth:             indentWidth,
		UseTab:                  useTab,
	}
	fmter := formatter.NewFormatter(settings, sugar)
	formattedQuery, err := fmter.Format(str)

	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	} else {
		fmt.Fprintln(os.Stdout, formattedQuery)
	}
}

func init() {
	cobra.OnInitialize()
	rootCmd.Flags().StringVarP(
		&indentCommaListsStr,
		"indentCommaLists",
		"c",
		"NoSpaceAfterComma",
		`choose whether or not you want to put a 'SpaceAfterComma', 'TrailingComma', 
        or 'NoSpaceAfterComma'.`,
	)
	rootCmd.Flags().BoolVarP(
		&indentInLists,
		"indentInLists",
		"l",
		false,
		"choose whether or not you want to indent in lists.",
	)
	rootCmd.Flags().BoolVarP(
		&indentBetweenConditions,
		"indentBetweenConditions",
		"b",
		false,
		"choose whether or not you want to indent between conditions.",
	)
	rootCmd.Flags().StringVarP(
		&keywordCaseStr,
		"keywordCase",
		"k",
		"UpperCase",
		"choose whether or not you want to make keywords 'UpperCase' or 'LowerCase'",
	)
	rootCmd.Flags().Uint32VarP(
		&maxWidth,
		"maxWidth",
		"m",
		80,
		"choose the max width of a line",
	)
	rootCmd.Flags().Uint32VarP(
		&indentWidth,
		"indentWidth",
		"w",
		4,
		"choose the width of indent",
	)
	rootCmd.Flags().BoolVarP(
		&useTab,
		"useTab",
		"u",
		false,
		"choose whether or not you want to use tab instead of spaces.",
	)
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		fmt.Printf("could not parse cmd ")
	}
}
