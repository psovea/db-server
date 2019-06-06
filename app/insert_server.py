class PromInsertServer:
    def __init__(self, port=8000):
        """Start the http server for scraping
        The port where you open should be scraped by prometheus
        e.g port 8000 could have:
        scrape_configs:
          - job_name: 'local'
            scrape_interval: 5s
            static_configs:
            - targets: ['localhost:8000']
        """
        start_http_server(port)
        self.data = []
        REGISTRY.register(self)

    def collect(self):
        """Is called when prometheus requests data from this server (it seems)
        when multiple series with the same labels are updated, only takes the
        latest one
        """
        for metric_name, value, labels, info, type_metric in self.data:
            metric = Metric(metric_name, info, type_metric)
            metric.add_sample(metric_name,
                              value=float(value), labels=labels)
            yield metric
        self.data = []

    def insert_into_prom(self, metric, value, labels, info="", type_metric='gauge'):
        """metric_name is the name of actual metric, should be what the metric represents
        value should either be a number (int, float) or string in the form of a number
        labels should be a dict of the labels, with the keys the label names and values the label values
        info is some info you want to add to the metric (can't find it in port 9090 though)
        type_metric is the type of the metric, e.g. counter, gauge or histogram
        """
        labels = dict(metric, value)
        self.data.append((metric, value, labels, info, type_metric))
