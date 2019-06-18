from prometheus_client import start_http_server, Metric, REGISTRY
# from kv6 import counters


class PromInsertServer:
    def __init__(self, port=8000, counters={}):
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
        # keep data for scrape_interval * scrape_amount
        # (scrape_interval is found in /etc/prometheus/prometheus.yml, when writing it is 5)
        self.scrape_amount = 60
        self.scrape_count = self.scrape_amount // 2
        self.data = [[] for _ in range(self.scrape_amount)]
        self.counters = counters
        REGISTRY.register(self)

    def collect(self):
        """Is called when prometheus requests data from this server (it seems)
        when multiple series with the same labels are updated, only takes the
        latest one
        """
        for lst in self.data[self.scrape_count:] + self.data[:self.scrape_count]:
            for metric_name, value, labels, info, type_metric in lst:
                metric = Metric(metric_name, info, type_metric)
                metric.add_sample(metric_name,
                                  value=float(value), labels=labels)
                yield metric
        self.scrape_count = (self.scrape_count - 1) % self.scrape_amount
        self.data[self.scrape_count] = []

        for labels, value in self.counters.items():
            metric = Metric('location_punctuality', '', 'counter')
            metric.add_sample('location_punctuality', value=float(
                value), labels=dict(labels))
            yield metric

    def insert_into_prom(self, metric, value, labels, info="", type_metric='gauge'):
        """metric_name is the name of actual metric, should be what the metric represents
        value should either be a number (int, float) or string in the form of a number
        labels should be a dict of the labels, with the keys the label names and values the label values
        info is some info you want to add to the metric (can't find it in port 9090 though)
        type_metric is the type of the metric, e.g. counter, gauge or histogram
        """
        self.data[self.scrape_count].append(
            (metric, value, labels, info, type_metric))
