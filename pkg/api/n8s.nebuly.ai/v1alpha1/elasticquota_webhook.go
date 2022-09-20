package v1alpha1

import (
	"context"
	"fmt"
	"github.com/nebuly-ai/nebulnetes/pkg/constant"
	"github.com/nebuly-ai/nebulnetes/pkg/util"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	. "sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// log is for logging in this package.
var eqlog = logf.Log.WithName("elasticquota-resource")

func (r *ElasticQuota) SetupWebhookWithManager(mgr ctrl.Manager) error {
	if client == nil {
		client = mgr.GetClient()
	}
	return ctrl.NewWebhookManagedBy(mgr).
		For(r).
		Complete()
}

//+kubebuilder:webhook:path=/validate-n8s-nebuly-ai-v1alpha1-elasticquota,mutating=false,failurePolicy=fail,sideEffects=None,groups=n8s.nebuly.ai,resources=elasticquotas,verbs=create,versions=v1alpha1,name=velasticquota.kb.io,admissionReviewVersions=v1

var _ webhook.Validator = &ElasticQuota{}

// ValidateCreate implements webhook.Validator so a webhook will be registered for the type
func (r *ElasticQuota) ValidateCreate() error {
	eqlog.V(1).Info("validate create", "name", r.Name)
	if client == nil {
		err := fmt.Errorf(constant.InternalErrorMsg)
		eqlog.Error(err, "client was not initialized correctly")
		return err
	}

	// Check if there's already another ElasticQuota in the same namespace
	var eqList ElasticQuotaList
	if err := client.List(context.Background(), &eqList, InNamespace(r.Namespace)); IgnoreNotFound(err) != nil {
		eqlog.Error(err, "unable to list elastic quotas")
		return fmt.Errorf(constant.InternalErrorMsg)
	}
	if len(eqList.Items) > 0 {
		return fmt.Errorf(
			"only 1 ElasticQuota per namespace is allowed - ElasticQuota %q already exists in namespace %q",
			eqList.Items[0].Name,
			r.Namespace,
		)
	}

	// Check if there's already a CompositeElasticQuota defining a quota for the ElasticQuota namespace
	var compositeEqList CompositeElasticQuotaList
	if err := client.List(context.Background(), &compositeEqList); err != nil {
		eqlog.Error(err, "unable to list composite elastic quotas")
		return fmt.Errorf(constant.InternalErrorMsg)
	}
	for _, compositeEq := range compositeEqList.Items {
		if util.InSlice(r.Namespace, compositeEq.Spec.Namespaces) {
			return fmt.Errorf("the CompositeElasticQuota \"%s/%s\" already defines quotas for namespace %q",
				compositeEq.Namespace,
				compositeEq.Name,
				r.Namespace,
			)
		}
	}

	return nil
}

// ValidateUpdate implements webhook.Validator so a webhook will be registered for the type
func (r *ElasticQuota) ValidateUpdate(old runtime.Object) error {
	return nil
}

// ValidateDelete implements webhook.Validator so a webhook will be registered for the type
func (r *ElasticQuota) ValidateDelete() error {
	return nil
}
