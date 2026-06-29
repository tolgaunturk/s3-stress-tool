#!/usr/bin/env python
"""
Command-line interface for Ceph S3 Load Testing Tool.
"""

import click
import boto3
import json
from datetime import datetime, timedelta
from pathlib import Path
from rich.console import Console
from rich.table import Table
from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.progress import Progress

from scenarios import SCENARIOS, get_scenario_info
from engine import TestEngine
from reporter import Reporter

console = Console()
reporter = Reporter()


def format_duration(seconds):
    """Format seconds as HH:MM:SS."""
    delta = timedelta(seconds=int(seconds))
    return str(delta)


def format_bytes(bytes_val):
    """Format bytes as human readable."""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if bytes_val < 1024:
            return f"{bytes_val:.2f} {unit}"
        bytes_val /= 1024
    return f"{bytes_val:.2f} TB"


@click.group()
def cli():
    """Ceph S3 Load Testing Tool - CLI Edition"""
    pass


@cli.command()
def scenarios():
    """List all available test scenarios."""
    table = Table(title="Available Scenarios")
    table.add_column("Key", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Category", style="yellow")
    table.add_column("Concurrent", style="magenta")
    table.add_column("Object Size", style="blue")

    for key, scenario in SCENARIOS.items():
        size_mb = scenario['object_size'] / (1024 * 1024)
        table.add_row(
            key,
            scenario['name'],
            scenario['category'],
            str(scenario['concurrent_ops']),
            f"{size_mb:.1f} MB"
        )

    console.print(table)


@cli.command()
@click.option('--scenario', required=True, help='Scenario key')
@click.option('--bucket', required=True, help='S3 bucket name')
@click.option('--endpoint', required=True, help='S3 endpoint URL')
@click.option('--access-key', required=True, help='AWS access key')
@click.option('--secret-key', required=True, help='AWS secret key')
@click.option('--region', default='default', help='AWS region (default: default)')
def run(scenario, bucket, endpoint, access_key, secret_key, region):
    """Run a stress test scenario."""

    if scenario not in SCENARIOS:
        console.print(f"[red]Error: Unknown scenario '{scenario}'[/red]")
        return

    # Create S3 client
    try:
        s3_client = boto3.client(
            's3',
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        s3_client.head_bucket(Bucket=bucket)
    except Exception as e:
        console.print(f"[red]Error connecting to S3: {e}[/red]")
        return

    scenario_config = SCENARIOS[scenario]

    # Start test
    engine = TestEngine()
    engine.start(scenario_config, bucket, s3_client)

    console.print(f"[green]✓ Test started:[/green] {scenario_config['name']}")
    console.print(f"[dim]Running {scenario_config['concurrent_ops']} concurrent operations...[/dim]\n")

    # Live monitoring
    def generate_status():
        layout = Layout()
        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="metrics"),
            Layout(name="details"),
        )

        status = engine.get_status()
        elapsed = status['elapsed']
        iops = status['iops']
        throughput = status['throughput_mbs']
        latency = status['latency_avg']
        success_rate = status['success_rate']
        total_ops = status['total_ops']

        # Header
        duration_str = format_duration(elapsed)
        status_indicator = "● RUNNING" if status['running'] else "✓ COMPLETE"
        status_color = "green" if status['running'] else "blue"
        header_text = f"Scenario: {scenario_config['name']} | Duration: {duration_str} | Status: [{status_color}]{status_indicator}[/{status_color}]"
        layout['header'].update(Panel(header_text, expand=False))

        # Metrics
        metrics_text = f"""
IOPS: {iops:.2f}    Throughput: {throughput:.2f} MB/s    Latency: {latency:.2f}ms    Success: {success_rate:.1f}%
Total Ops: {total_ops:,}    Read IOPS: {status['read_iops']:.2f}    Write IOPS: {status['write_iops']:.2f}
Data: {status['data_transferred_mb']:.2f} MB    P50: {status['latency_p50']:.2f}ms    P99: {status['latency_p99']:.2f}ms
        """
        layout['metrics'].update(Panel(metrics_text.strip(), expand=True))

        # Details
        details = Table(show_header=False, box=None)
        details.add_row("Successful Ops", str(status['successful_ops']))
        details.add_row("Failed Ops", str(status['failed_ops']))
        details.add_row("Min Latency", f"{status['latency_min']:.2f}ms")
        details.add_row("Max Latency", f"{status['latency_max']:.2f}ms")
        layout['details'].update(Panel(details, expand=False))

        return layout

    try:
        with Live(generate_status, refresh_per_second=1, console=console) as live:
            while engine.is_running():
                live.update(generate_status())
    except KeyboardInterrupt:
        console.print("\n[yellow]Stopping test...[/yellow]")
        engine.stop()
        while engine.is_running():
            pass  # Wait for graceful shutdown

    # Final stats
    final_status = engine.get_status()
    console.print("\n[green bold]Test Complete![/green bold]\n")

    summary = Table(title="Final Results")
    summary.add_column("Metric", style="cyan")
    summary.add_column("Value", style="green")

    summary.add_row("Total Operations", str(final_status['total_ops']))
    summary.add_row("Successful", str(final_status['successful_ops']))
    summary.add_row("Failed", str(final_status['failed_ops']))
    summary.add_row("IOPS", f"{final_status['iops']:.2f}")
    summary.add_row("Read IOPS", f"{final_status['read_iops']:.2f}")
    summary.add_row("Write IOPS", f"{final_status['write_iops']:.2f}")
    summary.add_row("Throughput", f"{final_status['throughput_mbs']:.2f} MB/s")
    summary.add_row("Data Transferred", f"{final_status['data_transferred_mb']:.2f} MB")
    summary.add_row("Avg Latency", f"{final_status['latency_avg']:.2f}ms")
    summary.add_row("P50 Latency", f"{final_status['latency_p50']:.2f}ms")
    summary.add_row("P99 Latency", f"{final_status['latency_p99']:.2f}ms")
    summary.add_row("Success Rate", f"{final_status['success_rate']:.1f}%")
    summary.add_row("Duration", format_duration(final_status['elapsed']))

    console.print(summary)

    # Save report
    run_id = reporter.save_report(scenario, scenario_config, final_status, final_status.get('timeseries', []))
    console.print(f"\n[blue]Report saved:[/blue] {run_id}")


@cli.command()
def reports():
    """List all saved test reports."""
    reports_list = reporter.list_reports()

    if not reports_list:
        console.print("[yellow]No reports found[/yellow]")
        return

    table = Table(title="Saved Reports")
    table.add_column("Run ID", style="cyan")
    table.add_column("Scenario", style="green")
    table.add_column("IOPS", style="yellow")
    table.add_column("Throughput", style="magenta")
    table.add_column("Success Rate", style="blue")
    table.add_column("Timestamp", style="dim")

    for report in reports_list:
        table.add_row(
            report['run_id'],
            report['scenario'],
            f"{report['iops']:.2f}",
            f"{report['throughput_mbs']:.2f} MB/s",
            f"{report['success_rate']:.1f}%",
            report['timestamp']
        )

    console.print(table)


@cli.command()
@click.option('--run-id', required=True, help='Run ID to export')
@click.option('--format', type=click.Choice(['csv', 'json']), default='csv', help='Export format')
def export(run_id, format):
    """Export a report as CSV or JSON."""
    if format == 'csv':
        content = reporter.export_csv(run_id)
        filename = f"{run_id}.csv"
    else:
        content = reporter.export_json(run_id)
        filename = f"{run_id}.json"

    if not content:
        console.print(f"[red]Error: Report '{run_id}' not found[/red]")
        return

    with open(filename, 'w') as f:
        f.write(content)

    console.print(f"[green]✓ Exported to:[/green] {filename}")


@cli.command()
@click.option('--a', 'run_id_a', required=True, help='First run ID')
@click.option('--b', 'run_id_b', required=True, help='Second run ID')
def compare(run_id_a, run_id_b):
    """Compare two test runs."""
    comparison = reporter.compare_reports(run_id_a, run_id_b)

    if not comparison:
        console.print("[red]Error: One or both reports not found[/red]")
        return

    console.print(f"\n[cyan bold]Comparing {run_id_a} vs {run_id_b}[/cyan bold]\n")

    table = Table(title="Comparison Results")
    table.add_column("Metric", style="cyan")
    table.add_column("Run A", style="green")
    table.add_column("Run B", style="yellow")
    table.add_column("Delta %", style="magenta")

    comp = comparison['comparison']
    for metric_name, metric_data in comp.items():
        delta = metric_data['delta_percent']
        delta_str = f"{delta:+.2f}%"
        if delta > 0:
            delta_str = f"[green]{delta_str}[/green]"
        elif delta < 0:
            delta_str = f"[red]{delta_str}[/red]"

        table.add_row(
            metric_name,
            str(metric_data['a']),
            str(metric_data['b']),
            delta_str
        )

    console.print(table)


if __name__ == '__main__':
    cli()
