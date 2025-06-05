import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import time
from iceberg_catalog import create_iceberg_catalog
import warnings
warnings.filterwarnings('ignore')

class ExtendedManufacturingAnalytics:
    def __init__(self):
        print("🔄 Connecting to PyIceberg catalog...")
        self.catalog = create_iceberg_catalog()
        try:
            self.table = self.catalog.load_table("manufacturing.quality_inspections")
            print("✅ Connected to PyIceberg table for advanced analytics")
        except Exception as e:
            print(f"❌ Could not load PyIceberg table: {e}")
            self.table = None
    
    def analyze_hourly_trends(self, df=None):
        if df is None:
            df = self.table.scan().to_pandas()
        
        print("\n📊 HOURLY PRODUCTION ANALYSIS:")
        print("=" * 70)
        
        df['event_time'] = pd.to_datetime(df['event_time'])
        df['hour'] = df['event_time'].dt.floor('H')
        df['is_defect'] = df['result'] == 'NG'
        
        hourly_stats = df.groupby('hour').agg({
            'product_id': 'count',
            'is_defect': ['sum', 'mean'],
            'line_speed': 'mean',
            'temperature': 'mean',
            'humidity': 'mean'
        }).round(2)
        
        hourly_stats.columns = ['throughput', 'defects', 'defect_rate', 'avg_speed', 'avg_temp', 'avg_humidity']
        hourly_stats['defect_rate'] = (hourly_stats['defect_rate'] * 100).round(1)
        
        print("\n🕐 Hourly Production Metrics:")
        print("-" * 70)
        print(f"{'Hour':<20} {'Throughput':<12} {'Defects':<10} {'Rate %':<8} {'Speed':<8} {'Temp°C':<8} {'Humid%':<8}")
        print("-" * 70)
        
        for idx, row in hourly_stats.iterrows():
            hour_str = idx.strftime("%Y-%m-%d %H:00")
            print(f"{hour_str:<20} {row['throughput']:<12.0f} {row['defects']:<10.0f} "
                  f"{row['defect_rate']:<8.1f} {row['avg_speed']:<8.1f} "
                  f"{row['avg_temp']:<8.1f} {row['avg_humidity']:<8.1f}")
        
        if len(hourly_stats) > 1:
            throughput_trend = np.polyfit(range(len(hourly_stats)), hourly_stats['throughput'], 1)[0]
            defect_trend = np.polyfit(range(len(hourly_stats)), hourly_stats['defect_rate'], 1)[0]
            
            print(f"\n📈 Trends:")
            print(f"  Throughput: {'📈 Increasing' if throughput_trend > 0 else '📉 Decreasing'} "
                  f"({throughput_trend:.1f} units/hour)")
            print(f"  Defect Rate: {'🔴 Worsening' if defect_trend > 0 else '🟢 Improving'} "
                  f"({defect_trend:.2f}% per hour)")
    
    def analyze_defect_distributions(self, df=None):
        if df is None:
            df = self.table.scan().to_pandas()
        
        print("\n🔍 DEFECT DISTRIBUTION ANALYSIS:")
        print("=" * 70)
        
        defects_df = df[df['result'] == 'NG'].copy()
        total_defects = len(defects_df)
        
        if total_defects == 0:
            print("No defects found in the data!")
            return
        
        print("\n1️⃣ Defects by Type:")
        print("-" * 40)
        defect_types = defects_df['defect_type'].value_counts()
        for defect_type, count in defect_types.items():
            percentage = (count / total_defects) * 100
            bar = '█' * int(percentage / 2)
            print(f"  {defect_type:<20} {count:>4} ({percentage:>5.1f}%) {bar}")
        
        print("\n2️⃣ Defects by Inspection Station:")
        print("-" * 40)
        station_stats = df.groupby('inspection_station').agg({
            'product_id': 'count',
            'result': lambda x: (x == 'NG').sum()
        })
        station_stats['defect_rate'] = (station_stats['result'] / station_stats['product_id'] * 100).round(1)
        station_stats = station_stats.sort_values('defect_rate', ascending=False)
        
        for station, row in station_stats.iterrows():
            bar = '█' * int(row['defect_rate'] / 2)
            print(f"  {station:<15} {row['result']:>4} defects / {row['product_id']:>4} total "
                  f"({row['defect_rate']:>5.1f}%) {bar}")
        
        print("\n3️⃣ Defects by Operator:")
        print("-" * 40)
        operator_stats = df.groupby('operator_id').agg({
            'product_id': 'count',
            'result': lambda x: (x == 'NG').sum()
        })
        operator_stats['defect_rate'] = (operator_stats['result'] / operator_stats['product_id'] * 100).round(1)
        operator_stats = operator_stats.sort_values('defect_rate', ascending=False)
        
        for operator, row in operator_stats.iterrows():
            bar = '█' * int(row['defect_rate'] / 2)
            print(f"  {operator:<15} {row['result']:>4} defects / {row['product_id']:>4} total "
                  f"({row['defect_rate']:>5.1f}%) {bar}")
        
        if len(station_stats) > 1:
            best_station = station_stats['defect_rate'].idxmin()
            worst_station = station_stats['defect_rate'].idxmax()
            print(f"\n⚠️  Alert: {worst_station} has {station_stats.loc[worst_station, 'defect_rate']:.1f}% "
                  f"defect rate vs {best_station} with {station_stats.loc[best_station, 'defect_rate']:.1f}%")
    
    def analyze_shift_performance(self, df=None):
        if df is None:
            df = self.table.scan().to_pandas()
        
        print("\n⏰ SHIFT PERFORMANCE COMPARISON:")
        print("=" * 70)
        
        df['event_time'] = pd.to_datetime(df['event_time'])
        
        for shift in df['shift_id'].unique():
            shift_df = df[df['shift_id'] == shift].copy()
            
            if len(shift_df) < 10:
                continue
                
            print(f"\n📋 Shift: {shift}")
            print("-" * 40)
            
            shift_df = shift_df.sort_values('event_time')
            mid_point = len(shift_df) // 2
            
            first_half = shift_df.iloc[:mid_point]
            second_half = shift_df.iloc[mid_point:]
            
            metrics = {}
            for name, half_df in [("First Half", first_half), ("Second Half", second_half)]:
                defect_rate = (half_df['result'] == 'NG').mean() * 100
                avg_speed = half_df['line_speed'].mean()
                avg_confidence = half_df['confidence'].mean()
                
                metrics[name] = {
                    'count': len(half_df),
                    'defect_rate': defect_rate,
                    'avg_speed': avg_speed,
                    'avg_confidence': avg_confidence
                }
            
            print(f"\n{'Metric':<20} {'First Half':<15} {'Second Half':<15} {'Change':<15}")
            print("-" * 65)
            
            count_change = metrics['Second Half']['count'] - metrics['First Half']['count']
            print(f"{'Products Inspected':<20} {metrics['First Half']['count']:<15} "
                  f"{metrics['Second Half']['count']:<15} {count_change:+<15}")
            
            defect_change = metrics['Second Half']['defect_rate'] - metrics['First Half']['defect_rate']
            trend = "🔴 Worse" if defect_change > 0 else "🟢 Better"
            print(f"{'Defect Rate %':<20} {metrics['First Half']['defect_rate']:<15.1f} "
                  f"{metrics['Second Half']['defect_rate']:<15.1f} {defect_change:+.1f} {trend}")
            
            speed_change = metrics['Second Half']['avg_speed'] - metrics['First Half']['avg_speed']
            print(f"{'Avg Line Speed':<20} {metrics['First Half']['avg_speed']:<15.1f} "
                  f"{metrics['Second Half']['avg_speed']:<15.1f} {speed_change:+.1f}")
            
            conf_change = metrics['Second Half']['avg_confidence'] - metrics['First Half']['avg_confidence']
            print(f"{'Avg Confidence':<20} {metrics['First Half']['avg_confidence']:<15.3f} "
                  f"{metrics['Second Half']['avg_confidence']:<15.3f} {conf_change:+.3f}")
    
    def analyze_environmental_correlations(self, df=None):
        if df is None:
            df = self.table.scan().to_pandas()
        
        print("\n🌡️ ENVIRONMENTAL FACTOR ANALYSIS:")
        print("=" * 70)
        
        df['is_defect'] = (df['result'] == 'NG').astype(int)
        
        factors = ['temperature', 'humidity', 'line_speed']
        correlations = {}
        
        for factor in factors:
            corr = df['is_defect'].corr(df[factor])
            correlations[factor] = corr
        
        print("\n📊 Correlation with Defect Rate:")
        print("-" * 40)
        for factor, corr in correlations.items():
            strength = "Strong" if abs(corr) > 0.5 else "Moderate" if abs(corr) > 0.3 else "Weak"
            direction = "positive" if corr > 0 else "negative"
            print(f"  {factor.capitalize():<15} {corr:>7.3f} ({strength} {direction})")
        
        print("\n📈 Defect Rates by Environmental Conditions:")
        
        print("\n🌡️ Temperature Ranges:")
        temp_bins = pd.qcut(df['temperature'], q=4, labels=['Low', 'Medium-Low', 'Medium-High', 'High'])
        temp_analysis = df.groupby(temp_bins)['is_defect'].agg(['sum', 'count', 'mean'])
        temp_analysis['rate'] = (temp_analysis['mean'] * 100).round(1)
        
        for range_name, row in temp_analysis.iterrows():
            temp_range = df.groupby(temp_bins)['temperature'].agg(['min', 'max']).loc[range_name]
            print(f"  {range_name:<12} ({temp_range['min']:.1f}-{temp_range['max']:.1f}°C): "
                  f"{row['sum']:>3} defects / {row['count']:>4} total ({row['rate']:>5.1f}%)")
        
        print("\n💧 Humidity Ranges:")
        humid_bins = pd.qcut(df['humidity'], q=4, labels=['Low', 'Medium-Low', 'Medium-High', 'High'])
        humid_analysis = df.groupby(humid_bins)['is_defect'].agg(['sum', 'count', 'mean'])
        humid_analysis['rate'] = (humid_analysis['mean'] * 100).round(1)
        
        for range_name, row in humid_analysis.iterrows():
            humid_range = df.groupby(humid_bins)['humidity'].agg(['min', 'max']).loc[range_name]
            print(f"  {range_name:<12} ({humid_range['min']:.1f}-{humid_range['max']:.1f}%): "
                  f"{row['sum']:>3} defects / {row['count']:>4} total ({row['rate']:>5.1f}%)")
        
        print("\n⚡ Line Speed Ranges:")
        speed_bins = pd.qcut(df['line_speed'], q=4, labels=['Slow', 'Medium-Slow', 'Medium-Fast', 'Fast'])
        speed_analysis = df.groupby(speed_bins)['is_defect'].agg(['sum', 'count', 'mean'])
        speed_analysis['rate'] = (speed_analysis['mean'] * 100).round(1)
        
        for range_name, row in speed_analysis.iterrows():
            speed_range = df.groupby(speed_bins)['line_speed'].agg(['min', 'max']).loc[range_name]
            print(f"  {range_name:<12} ({speed_range['min']:.1f}-{speed_range['max']:.1f} u/m): "
                  f"{row['sum']:>3} defects / {row['count']:>4} total ({row['rate']:>5.1f}%)")
        
        print("\n✅ Optimal Operating Conditions:")
        optimal_temp = temp_analysis['rate'].idxmin()
        optimal_humid = humid_analysis['rate'].idxmin()
        optimal_speed = speed_analysis['rate'].idxmin()
        
        print(f"  Best Temperature Range: {optimal_temp}")
        print(f"  Best Humidity Range: {optimal_humid}")
        print(f"  Best Speed Range: {optimal_speed}")
    
    def generate_executive_summary(self, df=None):
        """Generate executive summary of manufacturing performance"""
        if df is None:
            df = self.table.scan().to_pandas()
        
        print("\n📊 EXECUTIVE SUMMARY:")
        print("=" * 70)
        
        total_products = len(df)
        total_defects = (df['result'] == 'NG').sum()
        overall_defect_rate = (total_defects / total_products * 100)
        
        print(f"\n🏭 Production Overview:")
        print(f"  Total Products Inspected: {total_products:,}")
        print(f"  Total Defects Found: {total_defects:,}")
        print(f"  Overall Defect Rate: {overall_defect_rate:.1f}%")
        
        df['event_time'] = pd.to_datetime(df['event_time'])
        time_span = df['event_time'].max() - df['event_time'].min()
        print(f"  Time Span: {time_span}")
        
        print(f"\n🔍 Key Findings:")
        
        station_defects = df.groupby('inspection_station').apply(
            lambda x: (x['result'] == 'NG').mean() * 100
        ).round(1)
        worst_station = station_defects.idxmax()
        print(f"  ⚠️  Highest defect rate at {worst_station}: {station_defects[worst_station]:.1f}%")
        
        defect_types = df[df['result'] == 'NG']['defect_type'].value_counts()
        if len(defect_types) > 0:
            top_defect = defect_types.index[0]
            top_defect_pct = (defect_types.iloc[0] / total_defects * 100)
            print(f"  ⚠️  Most common defect: {top_defect} ({top_defect_pct:.1f}% of all defects)")
        
        temp_corr = df['is_defect'].corr(df['temperature'])
        if abs(temp_corr) > 0.3:
            impact = "increases" if temp_corr > 0 else "decreases"
            print(f"  🌡️  Defect rate {impact} with temperature (correlation: {temp_corr:.3f})")
        
        print(f"\n💡 Recommendations:")
        print(f"  1. Focus quality improvements on {worst_station}")
        print(f"  2. Investigate root causes of {top_defect} defects")
        print(f"  3. Monitor environmental conditions during high defect periods")
        print(f"  4. Consider operator training for consistency across shifts")

def main():
    analytics = ExtendedManufacturingAnalytics()
    
    if analytics.table:
        print("\n🔄 Loading current manufacturing data...")
        current_df = analytics.table.scan().to_pandas()
        print(f"✅ Loaded {len(current_df)} inspection records")
        
        current_df['is_defect'] = (current_df['result'] == 'NG').astype(int)
        
        analytics.analyze_hourly_trends(current_df)
        analytics.analyze_defect_distributions(current_df)
        analytics.analyze_shift_performance(current_df)
        analytics.analyze_environmental_correlations(current_df)
        analytics.generate_executive_summary(current_df)
        
        print("\n✅ Analysis complete!")
        
if __name__ == "__main__":
    main()