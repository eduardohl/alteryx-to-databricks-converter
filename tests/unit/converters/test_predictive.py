"""Tests for predictive/ML converters."""

from __future__ import annotations

import pytest

from a2d.converters import ConverterRegistry
from a2d.ir.nodes import (
    ABAnalysisNode,
    AppendClusterNode,
    ARIMANode,
    BoostedModelNode,
    CountRegressionNode,
    CrossValidationNode,
    DecisionTreeNode,
    ETSNode,
    FindNearestNeighborsNode,
    ForestModelNode,
    GammaRegressionNode,
    KCentroidsDiagnosticsNode,
    KCentroidsNode,
    LiftChartNode,
    LinearRegressionNode,
    LogisticRegressionNode,
    MeansTestNode,
    ModelCoefficientsNode,
    ModelComparisonNode,
    NaiveBayesNode,
    NeuralNetworkNode,
    PrincipalComponentsNode,
    ScoreModelNode,
    SplineModelNode,
    StepwiseNode,
    SupportVectorMachineNode,
    TSForecastNode,
    VarianceInflationFactorsNode,
)

from .conftest import DEFAULT_CONFIG, make_node


class TestDecisionTreeConverter:
    def test_decision_tree(self):
        node = make_node(
            tool_type="DecisionTree",
            configuration={"TargetField": "label", "FeatureFields": "f1,f2,f3", "MaxDepth": "10"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, DecisionTreeNode)
        assert result.target_field == "label"
        assert result.feature_fields == ["f1", "f2", "f3"]
        assert result.max_depth == 10
        assert result.conversion_method == "mapping"


class TestForestModelConverter:
    def test_forest_model(self):
        node = make_node(
            tool_type="ForestModel",
            configuration={"TargetField": "target", "NumTrees": "200", "MaxDepth": "8"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ForestModelNode)
        assert result.target_field == "target"
        assert result.num_trees == 200
        assert result.max_depth == 8
        assert result.conversion_method == "mapping"


class TestLinearRegressionConverter:
    def test_linear_regression(self):
        node = make_node(
            tool_type="LinearRegression",
            configuration={"TargetField": "price", "FeatureFields": "sqft,beds", "Regularization": "0.1"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, LinearRegressionNode)
        assert result.target_field == "price"
        assert result.regularization == pytest.approx(0.1)
        assert result.conversion_method == "mapping"


class TestLogisticRegressionConverter:
    def test_logistic_regression(self):
        node = make_node(
            tool_type="LogisticRegression",
            configuration={"TargetField": "churn", "MaxIterations": "50"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, LogisticRegressionNode)
        assert result.target_field == "churn"
        assert result.max_iterations == 50
        assert result.conversion_method == "mapping"


class TestScoreModelConverter:
    def test_score_model(self):
        node = make_node(
            tool_type="ScoreModel",
            configuration={"ModelReference": "/models/my_model"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ScoreModelNode)
        assert result.model_reference == "/models/my_model"
        assert result.conversion_method == "mapping"


class TestBoostedModelConverter:
    def test_boosted_model(self):
        node = make_node(
            tool_type="BoostedModel",
            configuration={
                "TargetField": "label",
                "FeatureFields": "f1,f2,f3",
                "ModelType": "classification",
                "NumIterations": "200",
                "MaxDepth": "6",
                "LearningRate": "0.05",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, BoostedModelNode)
        assert result.target_field == "label"
        assert result.feature_fields == ["f1", "f2", "f3"]
        assert result.num_iterations == 200
        assert result.max_depth == 6
        assert result.learning_rate == pytest.approx(0.05)
        assert result.conversion_method == "mapping"


class TestNaiveBayesConverter:
    def test_naive_bayes(self):
        node = make_node(
            tool_type="NaiveBayes",
            configuration={"TargetField": "spam", "FeatureFields": "word1,word2", "Smoothing": "2.0"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, NaiveBayesNode)
        assert result.target_field == "spam"
        assert result.smoothing == pytest.approx(2.0)
        assert result.conversion_method == "mapping"


class TestSupportVectorMachineConverter:
    def test_svm(self):
        node = make_node(
            tool_type="SupportVectorMachine",
            configuration={"TargetField": "class", "Regularization": "0.5", "MaxIterations": "200"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, SupportVectorMachineNode)
        assert result.target_field == "class"
        assert result.regularization == pytest.approx(0.5)
        assert result.max_iterations == 200
        assert result.conversion_method == "mapping"


class TestNeuralNetworkConverter:
    def test_neural_network(self):
        node = make_node(
            tool_type="NeuralNetwork",
            configuration={"TargetField": "digit", "HiddenLayers": "128,64,32", "MaxIterations": "50"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, NeuralNetworkNode)
        assert result.target_field == "digit"
        assert result.hidden_layers == [128, 64, 32]
        assert result.max_iterations == 50
        assert result.conversion_method == "mapping"


class TestGammaRegressionConverter:
    def test_gamma_regression(self):
        node = make_node(
            tool_type="GammaRegression",
            configuration={"TargetField": "cost", "FeatureFields": "age,income", "LinkFunction": "inverse"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, GammaRegressionNode)
        assert result.target_field == "cost"
        assert result.link_function == "inverse"
        assert result.conversion_method == "mapping"


class TestCountRegressionConverter:
    def test_count_regression(self):
        node = make_node(
            tool_type="CountRegression",
            configuration={"TargetField": "visits", "FeatureFields": "age,gender"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, CountRegressionNode)
        assert result.target_field == "visits"
        assert result.link_function == "log"
        assert result.conversion_method == "mapping"


class TestSplineModelConverter:
    def test_spline_model(self):
        node = make_node(
            tool_type="SplineModel",
            configuration={"TargetField": "price", "MaxKnots": "15"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, SplineModelNode)
        assert result.target_field == "price"
        assert result.max_knots == 15
        assert result.conversion_method == "mapping"


class TestStepwiseConverter:
    def test_stepwise(self):
        node = make_node(
            tool_type="Stepwise",
            configuration={"TargetField": "outcome", "FeatureFields": "x1,x2,x3", "Direction": "forward"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, StepwiseNode)
        assert result.target_field == "outcome"
        assert result.direction == "forward"
        assert result.conversion_method == "mapping"


class TestKCentroidsConverter:
    def test_k_centroids(self):
        node = make_node(
            tool_type="KCentroids",
            configuration={"FeatureFields": "x,y,z", "K": "8", "MaxIterations": "50"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, KCentroidsNode)
        assert result.feature_fields == ["x", "y", "z"]
        assert result.k == 8
        assert result.max_iterations == 50
        assert result.conversion_method == "mapping"


class TestPrincipalComponentsConverter:
    def test_pca(self):
        node = make_node(
            tool_type="PrincipalComponents",
            configuration={"FeatureFields": "a,b,c,d", "NumComponents": "3"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, PrincipalComponentsNode)
        assert result.feature_fields == ["a", "b", "c", "d"]
        assert result.num_components == 3
        assert result.conversion_method == "mapping"


class TestCrossValidationConverter:
    def test_cross_validation(self):
        node = make_node(
            tool_type="CrossValidation",
            configuration={"NumFolds": "10", "ModelReference": "/models/lr"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, CrossValidationNode)
        assert result.num_folds == 10
        assert result.model_reference == "/models/lr"
        assert result.conversion_method == "mapping"


class TestARIMAConverter:
    def test_arima(self):
        node = make_node(
            tool_type="ARIMA",
            configuration={"TimeField": "date", "ValueField": "sales", "P": "2", "D": "1", "Q": "1"},
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ARIMANode)
        assert result.time_field == "date"
        assert result.value_field == "sales"
        assert result.p == 2
        assert result.d == 1
        assert result.q == 1
        assert result.conversion_method == "mapping"


class TestETSConverter:
    def test_ets(self):
        node = make_node(
            tool_type="ETS",
            configuration={
                "TimeField": "month",
                "ValueField": "revenue",
                "ErrorType": "multiplicative",
                "TrendType": "additive",
                "SeasonalType": "multiplicative",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ETSNode)
        assert result.time_field == "month"
        assert result.value_field == "revenue"
        assert result.error_type == "multiplicative"
        assert result.trend_type == "additive"
        assert result.seasonal_type == "multiplicative"
        assert result.conversion_method == "mapping"


class TestABAnalysisConverter:
    def test_ab_analysis(self):
        node = make_node(
            tool_type="ABAnalysis",
            configuration={
                "TreatmentField": "variant",
                "ResponseField": "conversion_rate",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ABAnalysisNode)
        assert result.treatment_field == "variant"
        assert result.response_field == "conversion_rate"
        assert result.conversion_method == "mapping"


class TestAppendClusterConverter:
    def test_append_cluster(self):
        node = make_node(
            tool_type="AppendCluster",
            configuration={
                "ModelReference": "kmeans_model_1",
                "FeatureFields": "Feature1,Feature2",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, AppendClusterNode)
        assert result.model_reference == "kmeans_model_1"
        assert result.feature_fields == ["Feature1", "Feature2"]
        assert result.conversion_method == "mapping"


class TestFindNearestNeighborsConverter:
    def test_find_nearest_neighbors(self):
        node = make_node(
            tool_type="FindNearestNeighbors",
            configuration={
                "FeatureFields": "X,Y,Z",
                "K": "10",
                "DistanceMetric": "manhattan",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, FindNearestNeighborsNode)
        assert result.feature_fields == ["X", "Y", "Z"]
        assert result.k == 10
        assert result.distance_metric == "manhattan"
        assert result.conversion_method == "mapping"


class TestKCentroidsDiagnosticsConverter:
    def test_k_centroids_diagnostics(self):
        node = make_node(
            tool_type="KCentroidsDiagnostics",
            configuration={
                "FeatureFields": "A,B,C",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, KCentroidsDiagnosticsNode)
        assert result.feature_fields == ["A", "B", "C"]
        assert result.conversion_method == "mapping"


class TestLiftChartConverter:
    def test_lift_chart(self):
        node = make_node(
            tool_type="LiftChart",
            configuration={
                "PredictionField": "predicted",
                "ActualField": "actual",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, LiftChartNode)
        assert result.prediction_field == "predicted"
        assert result.actual_field == "actual"
        assert result.conversion_method == "mapping"


class TestModelComparisonConverter:
    def test_model_comparison(self):
        node = make_node(
            tool_type="ModelComparison",
            configuration={
                "ModelReferences": "model_a,model_b,model_c",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ModelComparisonNode)
        assert result.model_references == ["model_a", "model_b", "model_c"]
        assert result.conversion_method == "mapping"


class TestModelCoefficientsConverter:
    def test_model_coefficients(self):
        node = make_node(
            tool_type="ModelCoefficients",
            configuration={
                "ModelReference": "linear_reg_1",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, ModelCoefficientsNode)
        assert result.model_reference == "linear_reg_1"
        assert result.conversion_method == "mapping"


class TestTSForecastConverter:
    def test_ts_forecast(self):
        node = make_node(
            tool_type="TSForecast",
            configuration={
                "TimeField": "date",
                "ValueField": "sales",
                "ForecastField": "predicted_sales",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, TSForecastNode)
        assert result.time_field == "date"
        assert result.value_field == "sales"
        assert result.forecast_field == "predicted_sales"
        assert result.conversion_method == "mapping"


class TestTestOfMeansConverter:
    def test_test_of_means(self):
        node = make_node(
            tool_type="TestOfMeans",
            configuration={
                "FieldA": "group_control",
                "FieldB": "group_treatment",
                "TestType": "paired",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, MeansTestNode)
        assert result.field_a == "group_control"
        assert result.field_b == "group_treatment"
        assert result.test_type == "paired"
        assert result.conversion_method == "mapping"


class TestVarianceInflationFactorsConverter:
    def test_vif(self):
        node = make_node(
            tool_type="VarianceInflationFactors",
            configuration={
                "FeatureFields": "X1,X2,X3,X4",
            },
        )
        result = ConverterRegistry.convert_node(node, DEFAULT_CONFIG)
        assert isinstance(result, VarianceInflationFactorsNode)
        assert result.feature_fields == ["X1", "X2", "X3", "X4"]
        assert result.conversion_method == "mapping"
