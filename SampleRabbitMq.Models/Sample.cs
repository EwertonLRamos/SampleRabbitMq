namespace SampleRabbitMq.Models;

public class Sample
{
    public int Id { get; set; }
    public string Name { get; set; } = "Sample";
    public List<SampleItem> Items { get; set; } = [];
}