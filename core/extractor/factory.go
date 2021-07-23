package extractor

type TableFactoryFn func() TableExtractor
type TopicFactoryFn func() TopicExtractor
type DashboardFactoryFn func() DashboardExtractor
type UserFactoryFn func() UserExtractor
type BucketFactoryFn func() BucketExtractor

type Factory struct {
	tableFnStore     map[string]TableFactoryFn
	topicFnStore     map[string]TopicFactoryFn
	dashboardFnStore map[string]DashboardFactoryFn
	userFnStore      map[string]UserFactoryFn
	bucketFnStore    map[string]BucketFactoryFn
}

func NewFactory() *Factory {
	return &Factory{
		tableFnStore:     make(map[string]TableFactoryFn),
		topicFnStore:     make(map[string]TopicFactoryFn),
		dashboardFnStore: make(map[string]DashboardFactoryFn),
		userFnStore:      make(map[string]UserFactoryFn),
		bucketFnStore:    make(map[string]BucketFactoryFn),
	}
}

func (f *Factory) Get(name string) (extractor interface{}, err error) {
	tableFn, ok := f.tableFnStore[name]
	if ok {
		return tableFn(), nil
	}

	topicFn, ok := f.topicFnStore[name]
	if ok {
		return topicFn(), nil
	}

	dashboardFn, ok := f.dashboardFnStore[name]
	if ok {
		return dashboardFn(), nil
	}

	userFn, ok := f.userFnStore[name]
	if ok {
		return userFn(), nil
  }
	bucketFn, ok := f.bucketFnStore[name]
	if ok {
		return bucketFn(), nil
	}

	return nil, NotFoundError{name}
}

func (f *Factory) SetTableExtractor(name string, fn TableFactoryFn) {
	f.tableFnStore[name] = fn
}

func (f *Factory) SetTopicExtractor(name string, fn TopicFactoryFn) {
	f.topicFnStore[name] = fn
}

func (f *Factory) SetDashboardExtractor(name string, fn DashboardFactoryFn) {
	f.dashboardFnStore[name] = fn
}

func (f *Factory) SetUserExtractor(name string, fn UserFactoryFn) {
	f.userFnStore[name] = fn
}
func (f *Factory) SetBucketExtractor(name string, fn BucketFactoryFn) {
	f.bucketFnStore[name] = fn
}
