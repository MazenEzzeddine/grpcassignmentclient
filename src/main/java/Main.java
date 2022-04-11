import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class Main
{
    public static void main(String[] args) {

        callForAssignment();

    }


    private static void callForAssignment() {
        ManagedChannel managedChannel = ManagedChannelBuilder.forAddress("assignmentservice", 5002)
                .usePlaintext()
                .build();

        AssignmentServiceGrpc.AssignmentServiceBlockingStub assignmentServiceBlockingStub = AssignmentServiceGrpc.newBlockingStub(managedChannel);
        AssignmentRequest request = AssignmentRequest.newBuilder().setRequest("Give me the Assignment plz").build();

        System.out.println("connected to server ");
        AssignmentResponse reply = assignmentServiceBlockingStub.getAssignment(request);





        System.out.println("We have the following consumers");
        for (Consumer c : reply.getConsumersList())
            System.out.println(c.getId());

        System.out.println("We have the following Assignmenet");

        for (Consumer c : reply.getConsumersList()) {
            System.out.println("Consumer has the following Assignment "+ c.getId() );
            for(Partition p : c.getAssignedPartitionsList()) {
                System.out.println("partition "+ p.getId() + " " + p.getArrivalRate() + " " + p.getLag() );

            }
        }



    }
}
