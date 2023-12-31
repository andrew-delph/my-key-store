package utils

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/exp/constraints"
)

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")

func randomString(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func GetMapKeys(inputMap map[string]interface{}) []string {
	keys := make([]string, 0, len(inputMap))
	for key := range inputMap {
		keys = append(keys, key)
	}
	return keys
}

func getLocalIP() (string, error) {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return "", err
	}
	for _, addr := range addrs {
		if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				return ipnet.IP.String(), nil
			}
		}
	}
	return "", fmt.Errorf("Local IP not found")
}

// EncodeInt64ToBytes encodes an int64 value to a byte slice using little-endian encoding.
func EncodeInt64ToBytes(value int64) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, value)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DecodeBytesToInt64 decodes an int64 value from a byte slice using little-endian encoding.
func DecodeBytesToInt64(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, errors.New("byte slice should be 8 bytes long for int64 decoding")
	}

	buf := bytes.NewReader(data)
	var result int64
	err := binary.Read(buf, binary.LittleEndian, &result)
	if err != nil {
		return 0, err
	}
	return result, nil
}

func RandomInt64() int64 {
	return rand.Int63()
}

func CalculateHash(input string) int {
	hasher := sha256.New()
	hasher.Write([]byte(input))
	hash := hasher.Sum(nil)

	// Convert the first 4 bytes of the hash to an int
	hashInt := int(binaryToUint32(hash[:4]))

	return hashInt
}

func binaryToUint32(b []byte) uint32 {
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

func compare2dBytes(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

func TrackTime(start time.Time, limit time.Duration, name string) {
	elapsed := time.Since(start)
	if elapsed > limit {
		logrus.Warnf("trackTime: %s took %.2f seconds", name, elapsed.Seconds())
	}
}

func WriteChannelTimeout(ch chan interface{}, value interface{}, timeoutSeconds int) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logrus.Error("WRITE CHANNEL CLOSED")
			err = errors.New("channel is closed")
		}
	}()
	select {
	case ch <- value:
		return nil
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		logrus.Error("WRITE TIMEOUT")
		return errors.New("write operation timed out")
	}
}

func RecieveChannelTimeout(ch chan interface{}, timeoutSeconds int) interface{} {
	select {
	case value, ok := <-ch:
		if !ok {
			logrus.Error("READ CHANNEL CLOSED")
			return errors.New("channel closed")
		}
		return value
	case <-time.After(time.Duration(timeoutSeconds) * time.Second):
		logrus.Error("READ TIMEOUT")
		return errors.New("receive operation timed out")
	}
}

func CompareStringList(listA, listB []string) bool {
	sort.Strings(listA)
	sort.Strings(listB)

	return reflect.DeepEqual(listA, listB)
}
