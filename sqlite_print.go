package sqllogictest

import (
	"math"
	"strconv"
	"strings"
)

// For conversions between TEXT and REAL storage classes, only the first 15 significant decimal digits of the number are preserved
func Sqlite3PrintFloat(f float64) string {
	sign := ""

	if f < 0.0 {
		sign = "-"
		f = -f
	} else if f == 0.0 {
		return "0.000"
	}
	if math.IsInf(f, -1) {
		return "-Inf"
	}
	if math.IsInf(f, 1) {
		return "Inf"
	}
	if math.IsNaN(f) {
		return "NaN"
	}

	// Multiply r by powers of ten until it lands somewhere in between 1.0e+19 and 1.0e+17.
	ff_h := f
	ff_l := 0.0
	exp := 0
	if ff_h > 9.223372036854774784e+18 {
		for ff_h > 9.223372036854774784e+118 {
			exp += 100
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e-100, -1.99918998026028836196e-117)
		}
		for ff_h > 9.223372036854774784e+28 {
			exp += 10
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e-10, -3.6432197315497741579e-27)
		}
		for ff_h > 9.223372036854774784e+18 {
			exp += 1
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e-01, -5.5511151231257827021e-18)
		}
	} else {
		for ff_h < 9.223372036854774784e-83 {
			exp -= 100
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e+100, -1.5902891109759918046e+83)
		}
		for ff_h < 9.223372036854774784e+07 {
			exp -= 10
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e+10, 0.0)
		}
		for ff_h < 9.22337203685477478e+17 {
			exp -= 1
			ff_h, ff_l = dekkerMul2(ff_h, ff_l, 1.0e+01, 0.0)
		}
	}

	v := uint64(0)
	if ff_l < 0.0 {
		v = uint64(ff_h) - uint64(-ff_l)
	} else {
		v = uint64(ff_h) + uint64(ff_l)
	}

	res := make([]byte, 0)
	for v != 0 {
		res = append([]byte{byte(v % 10)}, res...)
		v /= 10
	}

	// Add suffix zeros
	for k := 0; k < exp; k++ {
		res = append(res, 0)
	}

	// Add prefix zeros
	for k := 0; k > exp; k-- {
		res = append([]byte{0}, res...)
	}

	// TODO: In the original code there is rounding for the precision lost due to 3 decimals after DP

	// Round to 15 significant digits
	firstSD := 0
	for firstSD < len(res) {
		if res[firstSD] != 0 {
			break
		}
		firstSD++
	}

	j := firstSD + 16 // TODO: Might require additional work to handle smaller numbers but works for the existing test cases
	if len(res)-1 > j && res[j] >= 5 {
		for {
			j--
			res[j] += 1
			if res[j] <= 9 {
				break
			}
			res[j] = 0
		}
	}
	for j = firstSD + 16; j < len(res); j++ {
		res[j] = 0
	}

	ret := ""
	for n := 0; n < len(res); n++ {
		ret += strconv.Itoa(int(res[n]))
		if n+1 == len(res)+exp {
			ret += "."
		}
	}

	// Return 3 digits after DP
	dp := strings.Index(ret, ".")
	if dp == -1 {
		ret += "."
		dp = len(ret) - 1
	}
	for len(ret) < dp+4 {
		ret += "0"
	}

	// Remove leading zeroes
	p := 0
	for p < dp-1 && ret[p] == '0' {
		p++
	}
	ret = ret[p:]

	// Remove trailing zeroes beyond 3 + DP
	dp = strings.Index(ret, ".")
	ret = ret[:dp+4]

	return sign + ret
}

func dekkerMul2(xh, xl, yh, yl float64) (float64, float64) {
	mx := math.Float64bits(xh)
	mx &= 0xfffffffffc000000
	x := math.Float64frombits(mx)

	my := math.Float64bits(yh)
	my &= 0xfffffffffc000000
	y := math.Float64frombits(my)

	tx := xh - x
	ty := yh - y

	p := x * y
	q := x*ty + tx*y
	c := p + q
	cc := p - c + q + tx*ty
	cc = xh*yl + xl*yh + cc
	xh = c + cc
	xl = c - xh
	xl += cc
	return xh, xl
}
