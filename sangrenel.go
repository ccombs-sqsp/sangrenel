package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

type config struct {
	brokers               []string
	topic                 string
	group                 string
	compression           sarama.CompressionCodec
	compressionName       string
	requiredAcks          sarama.RequiredAcks
	requiredAcksName      string
	workers               int
	writersPerWorker      int
	interval              int
	kafkaVersionString    string
	tls                   bool
	tlsCaCertificate      string
	tlsCertificate        string
	tlsPrivateKey         string
	tlsInsecureSkipVerify bool
	sasl                  bool
	scramAlgorithmString  string
	saslUsername          string
	saslPassword          string
}

var (
	Config = &config{}

	// Character selection for random messages.
	chars = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$^&*(){}][:<>.")

	// Counters / misc.
	sentCnt uint64
	errCnt  uint64

	validKafkaVersions = []string{
		"0.8.2.0",
		"0.8.2.1",
		"0.8.2.2",
		"0.9.0.0",
		"0.9.0.1",
		"0.10.0.0",
		"0.10.0.1",
		"0.10.1.0",
		"0.10.1.1",
		"0.10.2.0",
		"0.10.2.1",
		"0.10.2.2",
		"0.11.0.0",
		"0.11.0.1",
		"0.11.0.2",
		"1.0.0",
		"1.0.1",
		"1.0.2",
		"1.1.0",
		"1.1.1",
		"2.0.0",
		"2.0.1",
		"2.1.0",
		"2.1.1",
		"2.2.0",
		"2.2.1",
		"2.2.2",
		"2.3.0",
		"2.3.1",
		"2.4.0",
		"2.4.1",
		"2.5.0",
		"2.5.1",
		"2.6.0",
		"2.6.1",
		"2.6.2",
		"2.7.0",
		"2.7.1",
		"2.8.0",
		"2.8.1",
		"3.0.0",
		"3.1.0"}

	validScramAlgorithms = map[string]bool{
		"SCRAM-SHA-512": true,
		"SCRAM-SHA-256": true}
)

func init() {
	flag.StringVar(&Config.topic, "topic", "sqsp-kafka-canary", "Kafka topic to produce to")
	flag.StringVar(&Config.topic, "group", "sqsp-kafka-canary-group", "Kafka group to use for our consumer")
	flag.StringVar(&Config.compressionName, "compression", "none", "Message compression: none, gzip, snappy")
	flag.StringVar(&Config.requiredAcksName, "required-acks", "local", "RequiredAcks config: none, local, all")
	flag.IntVar(&Config.workers, "workers", 1, "Number of workers")
	flag.IntVar(&Config.writersPerWorker, "writers-per-worker", 1, "Number of writer (Kafka producer) goroutines per worker")
	flag.IntVar(&Config.interval, "interval", 5, "Seconds to wait between attempts to connect to Kafka")
	brokerString := flag.String("brokers", "localhost:9092", "Comma delimited list of Kafka brokers")
	flag.StringVar(&Config.kafkaVersionString, "api-version", "", "Explicit sarama.Version string")
	flag.BoolVar(&Config.tls, "tls", false, "Whether to enable TLS communcation")
	flag.StringVar(&Config.tlsCaCertificate, "tls-ca-cert", "", "Path to the CA SSL certificate")
	flag.StringVar(&Config.tlsCertificate, "tls-cert-file", "", "Path to the certificate file")
	flag.StringVar(&Config.tlsPrivateKey, "tls-key-file", "", "Path to the private key file")
	flag.BoolVar(&Config.tlsInsecureSkipVerify, "tls-insecure-skip-verify", false, "TLS insecure skip verify")
	flag.BoolVar(&Config.sasl, "sasl", false, "Whether to enable SASL SCRAM communication")
	flag.StringVar(&Config.scramAlgorithmString, "scram-algorithm", "", "which algorithm of SASL SCRAM to use, e.g. SCRAM-SHA-512")
	flag.StringVar(&Config.saslUsername, "username", "", "Username to use when authenticating with SASL SCRAM")
	flag.StringVar(&Config.saslPassword, "password", "", "Password to use when authenticating with SASL SCRAM")
	flag.Parse()

	Config.brokers = strings.Split(*brokerString, ",")

	switch Config.compressionName {
	case "gzip":
		Config.compression = sarama.CompressionGZIP
	case "snappy":
		Config.compression = sarama.CompressionSnappy
	case "none":
		Config.compression = sarama.CompressionNone
	default:
		fmt.Printf("Invalid compression option: %s\n", Config.compressionName)
		os.Exit(1)
	}

	switch Config.requiredAcksName {
	case "none":
		Config.requiredAcks = sarama.NoResponse
	case "local":
		Config.requiredAcks = sarama.WaitForLocal
	case "all":
		Config.requiredAcks = sarama.WaitForAll
	default:
		fmt.Printf("Invalid required-acks option: %s\n", Config.requiredAcksName)
		os.Exit(1)
	}

}

func parseKafkaVersion(kafkaVersion string) sarama.KafkaVersion {
	version, err := sarama.ParseKafkaVersion(kafkaVersion)
	if err != nil {
		fmt.Printf("Invalid API version option: %s\n", Config.kafkaVersionString)
		fmt.Printf("Options: %+q\n", validKafkaVersions)
		os.Exit(1)
	}
	return version
}

// TODO: Handle case where topic doesn't exist on broker.

func main() {
	if graphiteIp != "" {
		go graphiteWriter()
	}

	go promWriter()

	version := Config.kafkaVersionString

	// Print Sangrenel startup info.
	fmt.Printf("\nStarting sqsp-kafka-canary")
	fmt.Printf("\nStarting %d client workers, %d writers per worker\n", Config.workers, Config.writersPerWorker)
	fmt.Printf("API Version: %s, Compression: %s, RequiredAcks: %s\n",
		version, Config.compressionName, Config.requiredAcksName)

	// Start client workers.
	for {
		canaryProduce()
		canaryConsume()
		fmt.Printf("Canary sent message successfully!\n")
		time.Sleep(time.Second * time.Duration(Config.interval))
	}
}

func constructConfig(isProducer bool) *sarama.Config {
	conf := sarama.NewConfig()
	conf.Version = parseKafkaVersion(Config.kafkaVersionString)

	if Config.sasl {
		log.Println("Sasl SCRAM enbaled!")
		if !validScramAlgorithms[Config.scramAlgorithmString] {
			fmt.Printf("Invalid SCRAM algorithm option: %s\n", Config.scramAlgorithmString)
			fmt.Printf("Options: %+q\n", keysOfMap(validScramAlgorithms))
			os.Exit(1)
		}
		if len(Config.saslUsername) == 0 {
			fmt.Printf("SASL requires a username!\n")
			os.Exit(1)
		}
		if len(Config.saslPassword) == 0 {
			fmt.Printf("SASL requires a password!\n")
			os.Exit(1)
		}
		conf.Metadata.Full = true
		conf.Net.SASL.Enable = true
		conf.Net.SASL.User = Config.saslUsername
		conf.Net.SASL.Password = Config.saslPassword
		conf.Net.SASL.Handshake = true
		if Config.scramAlgorithmString == "SCRAM-SHA-512" {
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA512} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		} else if Config.scramAlgorithmString == "SCRAM-SHA-256" {
			conf.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &XDGSCRAMClient{HashGeneratorFcn: SHA256} }
			conf.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		}
		// TLS
		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	if Config.tls {

		tlsConfig := &tls.Config{
			InsecureSkipVerify: Config.tlsInsecureSkipVerify,
		}

		if len(Config.tlsCaCertificate) > 0 {
			caCert, err := ioutil.ReadFile(Config.tlsCaCertificate)
			if err != nil {
				log.Println(err)
				os.Exit(1)
			}
			caCertPool := x509.NewCertPool()
			caCertPool.AppendCertsFromPEM(caCert)
			tlsConfig.RootCAs = caCertPool
		}

		if len(Config.tlsCertificate) > 0 && len(Config.tlsPrivateKey) > 0 {
			cert, err := tls.LoadX509KeyPair(Config.tlsCertificate, Config.tlsPrivateKey)
			if err != nil {
				log.Fatal(err)
			}
			tlsConfig.Certificates = []tls.Certificate{cert}
		}

		conf.Net.TLS.Enable = true
		conf.Net.TLS.Config = tlsConfig
	}

	if isProducer {
		conf.Producer.Compression = Config.compression
		conf.Producer.Return.Successes = true
		conf.Producer.RequiredAcks = Config.requiredAcks
	} else {

	}

	return conf
}

// he do be producing
func canaryProduce() {
	conf := constructConfig(true)
	client, err := sarama.NewClient(Config.brokers, conf)
	if err != nil {
		log.Println(err)
		return
	} else {
		log.Printf("producer client constructed\n")
	}

	// Init the producer.
	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Println(err.Error())
	}
	defer producer.Close()
	s := get_canary_sound()
	buf := make([]byte, len(s))
	for i := range buf {
		buf[i] = s[i]
	}
	_, _, err = producer.SendMessage(&sarama.ProducerMessage{Topic: Config.topic, Value: sarama.ByteEncoder(buf)})
	if err != nil {
		println(err)
	}
	recordMetrics()
}

func canaryConsume() {
	conf := constructConfig(false)
	client, err := sarama.NewClient(Config.brokers, conf)
	if err != nil {
		log.Println(err)
		return
	} else {
		log.Printf("consumer client constructed\n")
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Println(err.Error())
	}
	defer consumer.Close()

}

func get_canary_sound() string {
	// TODO: CHEEP CHEEP
	sounds := []string{
		"Borrowed feathers do not make fine birds.",
		"https://www.youtube.com/watch?v=MhmC4EazbaY",
		"chirp chirp",
		"cheep cheep",
		"chicka-dee-dee-dee",
	}
	return sounds[rand.Intn(len(sounds))]
}

// // writer generates random messages and write to Kafka.
// // Each wrtier belongs to a parent worker. Writers
// // throttle writes according to a global rate limiter
// // and report write throughput statistics up through
// // a shared tachymeter.
// func writer(c sarama.Client, t *tachymeter.Tachymeter) {
// 	// Init the producer.
// 	producer, err := sarama.NewSyncProducerFromClient(c)
// 	if err != nil {
// 		log.Println(err.Error())
// 	}
// 	defer producer.Close()

// 	source := rand.NewSource(time.Now().UnixNano())
// 	generator := rand.New(source)
// 	msgBatch := make([]*sarama.ProducerMessage, 0, Config.batchSize)

// 	for {
// 		// Message rate limiting works by having all writer loops incrementing
// 		// a global counter and tracking the aggregate per-second progress.
// 		// If the configured rate is met, the worker will sleep
// 		// for the remainder of the 1 second window.
// 		intervalEnd := time.Now().Add(time.Second)
// 		countStart := atomic.LoadUint64(&sentCnt)

// 		var sendTime time.Time
// 		var intervalSent uint64

// 		for {
// 			// Estimate the batch size. This should shrink
// 			// if we're near the rate limit. Estimated batch size =
// 			// amount left to send for this interval / number of writers
// 			// we have available to send this amount. If the estimate
// 			// is lower than the configured batch size, send that amount
// 			// instead.
// 			toSend := (Config.msgRate - intervalSent) / uint64((Config.workers * Config.writersPerWorker))
// 			n := int(math.Min(float64(toSend), float64(Config.batchSize)))

// 			for i := 0; i < n; i++ {
// 				// Gen message.
// 				msgData := make([]byte, Config.msgSize)
// 				randMsg(msgData, *generator)
// 				msg := &sarama.ProducerMessage{Topic: Config.topic, Value: sarama.ByteEncoder(msgData)}
// 				// Append to batch.
// 				msgBatch = append(msgBatch, msg)
// 			}

// 			sendTime = time.Now()
// 			err = producer.SendMessages(msgBatch)
// 			if err != nil {
// 				// Sarama returns a ProducerErrors, which is a slice
// 				// of errors per message errored. Use this count
// 				// to establish an error rate.
// 				atomic.AddUint64(&errCnt, uint64(len(err.(sarama.ProducerErrors))))
// 			}

// 			t.AddTime(time.Since(sendTime))
// 			atomic.AddUint64(&sentCnt, uint64(len(msgBatch)))

// 			msgBatch = msgBatch[:0]

// 			intervalSent = atomic.LoadUint64(&sentCnt) - countStart

// 			// Break if the global rate limit was met, or, if
// 			// we'd exceed it assuming all writers wrote a max batch size
// 			// for this interval.
// 			sendEstimate := intervalSent + uint64((Config.batchSize*Config.workers*Config.writersPerWorker)-Config.writersPerWorker)
// 			if sendEstimate >= Config.msgRate {
// 				break
// 			}
// 		}

// 		// If the global per-second rate limit was met,
// 		// the inner loop breaks and the outer loop sleeps for the interval remainder.
// 		time.Sleep(intervalEnd.Sub(time.Now()))
// 	}
// }

// // dummyWriter is initialized by the worker(s) if Config.noop is True.
// // dummyWriter performs the message generation step of the normal writer,
// // but doesn't connect to / attempt to send anything to Kafka. This is used
// // purely for testing message generation performance.
// func dummyWriter(t *tachymeter.Tachymeter) {
// 	source := rand.NewSource(time.Now().UnixNano())
// 	generator := rand.New(source)
// 	msgBatch := make([]*sarama.ProducerMessage, 0, Config.batchSize)

// 	for {
// 		for i := 0; i < Config.batchSize; i++ {
// 			// Gen message.
// 			msgData := make([]byte, Config.msgSize)
// 			randMsg(msgData, *generator)
// 			msg := &sarama.ProducerMessage{Topic: Config.topic, Value: sarama.ByteEncoder(msgData)}
// 			// Append to batch.
// 			msgBatch = append(msgBatch, msg)
// 		}

// 		atomic.AddUint64(&sentCnt, uint64(len(msgBatch)))
// 		t.AddTime(time.Duration(0))

// 		msgBatch = msgBatch[:0]
// 	}
// }

// // randMsg returns a random message generated from the chars byte slice.
// // Message length of m bytes as defined by Config.msgSize.
// func randMsg(m []byte, generator rand.Rand) {
// 	for i := range m {
// 		m[i] = chars[generator.Intn(len(chars))]
// 	}
// }

// // calcOutput takes a duration t and messages sent
// // and returns message rates in human readable network speeds.
// func calcOutput(t float64, n uint64) (float64, string) {
// 	m := (float64(n) / t) * float64(Config.msgSize)
// 	var o string
// 	switch {
// 	case m >= 131072:
// 		o = strconv.FormatFloat(m/131072, 'f', 0, 64) + "Mb/sec"
// 	case m < 131072:
// 		o = strconv.FormatFloat(m/1024, 'f', 0, 64) + "KB/sec"
// 	}
// 	return m, o
// }

// func round(t time.Duration) time.Duration {
// 	return t / 1000 * 1000
// }

func keysOfMap(m map[string]bool) []string {
	i := 0
	keys := make([]string, len(m))
	for k := range m {
		keys[i] = k
		i++
	}
	return keys
}
