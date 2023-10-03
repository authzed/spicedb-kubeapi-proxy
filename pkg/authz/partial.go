package authz

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// listOrObjectMeta is a superset of fields that may be returned by a kube
// request. It can then be converted to a standard type after inspection to
// see what fields were deserialized.
type listOrObjectMeta struct {
	ResourceVersion            string                      `json:"resourceVersion,omitempty" protobuf:"bytes,2,opt,name=resourceVersion"`
	Continue                   string                      `json:"continue,omitempty" protobuf:"bytes,3,opt,name=continue"`
	RemainingItemCount         *int64                      `json:"remainingItemCount,omitempty" protobuf:"bytes,4,opt,name=remainingItemCount"`
	Name                       string                      `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	GenerateName               string                      `json:"generateName,omitempty" protobuf:"bytes,2,opt,name=generateName"`
	Namespace                  string                      `json:"namespace,omitempty" protobuf:"bytes,3,opt,name=namespace"`
	SelfLink                   string                      `json:"selfLink,omitempty" protobuf:"bytes,4,opt,name=selfLink"`
	UID                        types.UID                   `json:"uid,omitempty" protobuf:"bytes,5,opt,name=uid,casttype=k8s.io/kubernetes/pkg/types.UID"`
	Generation                 int64                       `json:"generation,omitempty" protobuf:"varint,7,opt,name=generation"`
	CreationTimestamp          metav1.Time                 `json:"creationTimestamp,omitempty" protobuf:"bytes,8,opt,name=creationTimestamp"`
	DeletionTimestamp          *metav1.Time                `json:"deletionTimestamp,omitempty" protobuf:"bytes,9,opt,name=deletionTimestamp"`
	DeletionGracePeriodSeconds *int64                      `json:"deletionGracePeriodSeconds,omitempty" protobuf:"varint,10,opt,name=deletionGracePeriodSeconds"`
	Labels                     map[string]string           `json:"labels,omitempty" protobuf:"bytes,11,rep,name=labels"`
	Annotations                map[string]string           `json:"annotations,omitempty" protobuf:"bytes,12,rep,name=annotations"`
	OwnerReferences            []metav1.OwnerReference     `json:"ownerReferences,omitempty" patchStrategy:"merge" patchMergeKey:"uid" protobuf:"bytes,13,rep,name=ownerReferences"`
	Finalizers                 []string                    `json:"finalizers,omitempty" patchStrategy:"merge" protobuf:"bytes,14,rep,name=finalizers"`
	ManagedFields              []metav1.ManagedFieldsEntry `json:"managedFields,omitempty" protobuf:"bytes,17,rep,name=managedFields"`
}

func (m *listOrObjectMeta) ToListMeta() metav1.ListMeta {
	return metav1.ListMeta{
		ResourceVersion:    m.ResourceVersion,
		Continue:           m.Continue,
		RemainingItemCount: m.RemainingItemCount,
	}
}

func (m *listOrObjectMeta) ToObjectMeta() metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:                       m.Name,
		GenerateName:               m.GenerateName,
		Namespace:                  m.Namespace,
		UID:                        m.UID,
		ResourceVersion:            m.ResourceVersion,
		Generation:                 m.Generation,
		CreationTimestamp:          m.CreationTimestamp,
		DeletionTimestamp:          m.DeletionTimestamp,
		DeletionGracePeriodSeconds: m.DeletionGracePeriodSeconds,
		Labels:                     m.Labels,
		Annotations:                m.Annotations,
		OwnerReferences:            m.OwnerReferences,
		Finalizers:                 m.Finalizers,
		ManagedFields:              m.ManagedFields,
	}
}

type partialObjectOrList struct {
	metav1.TypeMeta  `json:",inline"`
	listOrObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	Items            []metav1.PartialObjectMetadata `json:"items" protobuf:"bytes,2,rep,name=items"`
}

func (p *partialObjectOrList) IsList() bool {
	return p.Items != nil
}

func (p *partialObjectOrList) ToPartialObjectMetadata() *metav1.PartialObjectMetadata {
	return &metav1.PartialObjectMetadata{
		TypeMeta:   p.TypeMeta,
		ObjectMeta: p.ToObjectMeta(),
	}
}

func (p *partialObjectOrList) ToPartialObjectMetadataList() *metav1.PartialObjectMetadataList {
	return &metav1.PartialObjectMetadataList{
		TypeMeta: p.TypeMeta,
		ListMeta: p.ToListMeta(),
		Items:    p.Items,
	}
}
