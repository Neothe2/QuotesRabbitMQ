from django.http import Http404
from django.shortcuts import render
import random
from rest_framework import viewsets, status
from rest_framework.response import Response
from rest_framework.views import APIView
from .models import *
from .serializers import *
from .producer import Producer
import threading

# Instantiate Producer
producer = Producer('localhost', default_queue='likes')


# Start heartbeat on a separate thread
heartbeat_thread = threading.Thread(target=producer.start_heartbeat, args=(10, 'likes', ))
heartbeat_thread.start()

# Create your views here.
class QuoteViewset(viewsets.ViewSet):
    def list(self, request):
        products = Quote.objects.all()
        serializer = QuoteSerializer(products, many=True)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def create(self, request):
        serializer = QuoteSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        producer.publish(serializer.data, method='quote_created')
        return Response(serializer.data, status=status.HTTP_201_CREATED)

    def retrieve(self, request, pk=None):
        product = Quote.objects.get(pk=pk)
        serializer = QuoteSerializer(product)
        return Response(serializer.data, status=status.HTTP_200_OK)

    def update(self, request, pk=None):
        product = Quote.objects.get(pk=pk)
        serializer = QuoteSerializer(instance=product, data=request.data)
        serializer.is_valid(raise_exception=True)
        serializer.save()
        producer.publish(serializer.data, method='quote_updated')
        return Response(serializer.data, status=status.HTTP_200_OK)

    def destroy(self, request, pk=None):
        product = Quote.objects.get(pk=pk)
        product.delete()
        producer.publish({'id': pk}, method='quote_deleted')
        return Response('Quote deleted')

class UserAPIView(APIView):
    def get(self, _):
        users = User.objects.all()
        return Response(UserSerializer(users, many=True).data)

class UserDetailAPIView(APIView):
    def get_user(self, pk):
        try:
            User.objects.get(pk=pk)
        except User.DoesNotExist:
            raise Http404

    def get(self, request, pk, format=None):
        user = self.get_user(pk)
        serializer = UserSerializer(user)
        return Response(serializer.data, status=status.HTTP_200_OK)
